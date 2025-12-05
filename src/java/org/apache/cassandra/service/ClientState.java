/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.db.virtual.VirtualSchemaKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MD5Digest;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_QUERY_HANDLER_CLASS;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * State related to a client connection.
 *
 * Base class for client state implementations:
 * - LocalClientState: Full implementation with AuthenticatedUser for local operations
 * - RemoteClientState: Serializable implementation with cached user attributes for remote operations
 */
public abstract class ClientState
{
    private static final Logger logger = LoggerFactory.getLogger(ClientState.class);

    public static final ImmutableSet<IResource> READABLE_SYSTEM_RESOURCES;
    public static final ImmutableSet<IResource> PROTECTED_AUTH_RESOURCES;

    static
    {
        // We want these system cfs to be always readable to authenticated users since many tools rely on them
        // (nodetool, cqlsh, bulkloader, etc.)
        ImmutableSet.Builder<IResource> readableBuilder = ImmutableSet.builder();
        for (String cf : Arrays.asList(SystemKeyspace.LOCAL, SystemKeyspace.LEGACY_PEERS, SystemKeyspace.PEERS_V2,
                                       SystemKeyspace.LEGACY_SIZE_ESTIMATES, SystemKeyspace.TABLE_ESTIMATES))
            readableBuilder.add(DataResource.table(SchemaConstants.SYSTEM_KEYSPACE_NAME, cf));

        // make all schema tables readable by default (required by the drivers)
        SchemaKeyspaceTables.ALL.forEach(table -> readableBuilder.add(DataResource.table(SchemaConstants.SCHEMA_KEYSPACE_NAME, table)));

        // make system_traces readable by all or else tracing will require explicit grants
        readableBuilder.add(DataResource.table(SchemaConstants.TRACE_KEYSPACE_NAME, TraceKeyspace.EVENTS));
        readableBuilder.add(DataResource.table(SchemaConstants.TRACE_KEYSPACE_NAME, TraceKeyspace.SESSIONS));

        // make all virtual schema tables readable by default as well
        VirtualSchemaKeyspace.instance.tables().forEach(t -> readableBuilder.add(t.metadata().resource));
        READABLE_SYSTEM_RESOURCES = readableBuilder.build();

        ImmutableSet.Builder<IResource> protectedBuilder = ImmutableSet.builder();
        // neither clients nor tools need authentication/authorization
        if (DatabaseDescriptor.isDaemonInitialized())
        {
            protectedBuilder.addAll(DatabaseDescriptor.getAuthenticator().protectedResources());
            protectedBuilder.addAll(DatabaseDescriptor.getAuthorizer().protectedResources());
            protectedBuilder.addAll(DatabaseDescriptor.getRoleManager().protectedResources());
        }

        PROTECTED_AUTH_RESOURCES = protectedBuilder.build();
    }

    // Current keyspace for the session
    private volatile String keyspace;

    private static final QueryHandler cqlQueryHandler;
    static
    {
        QueryHandler handler = QueryProcessor.instance;
        String customHandlerClass = CUSTOM_QUERY_HANDLER_CLASS.getString();
        if (customHandlerClass != null)
        {
            try
            {
                handler = FBUtilities.construct(customHandlerClass, "QueryHandler");
                logger.info("Using {} as a query handler for native protocol queries (as requested by the {} system property)",
                            customHandlerClass, CUSTOM_QUERY_HANDLER_CLASS.getKey());
            }
            catch (Exception e)
            {
                logger.error("Cannot use class {} as query handler", customHandlerClass, e);
                JVMStabilityInspector.killCurrentJVM(e, true);
            }
        }
        cqlQueryHandler = handler;
    }

    // isInternal is used to mark ClientState as used by some internal component
    // that should have an ability to modify system keyspace.
    public final boolean isInternal;

    // The biggest timestamp that was returned by getTimestamp/assigned to a query. This is global to ensure that the
    // timestamp assigned are strictly monotonic on a node, which is likely what user expect intuitively (more likely,
    // most new user will intuitively expect timestamp to be strictly monotonic cluster-wise, but while that last part
    // is unrealistic expectation, doing it node-wise is easy).
    private static final AtomicLong lastTimestampMicros = new AtomicLong(0);

    private boolean applyGuardrails = true;
    /**
     * Provides an additional control on the checking of guardrails. When executing SchemaTransformations in the
     * metadata log follower or when committing on a CMS member, we don't want guardrails to fire warnings.
     * @see org.apache.cassandra.schema.SchemaTransformation#enterExecution()
     **/
    public void pauseGuardrails()
    {
        applyGuardrails = false;
    }

    public void resumeGuardrails()
    {
        applyGuardrails = true;
    }

    public boolean applyGuardrails()
    {
        return applyGuardrails;
    }

    @VisibleForTesting
    public static void resetLastTimestamp(long nowMillis)
    {
        long nowMicros = TimeUnit.MILLISECONDS.toMicros(nowMillis);
        if (lastTimestampMicros.get() > nowMicros)
            lastTimestampMicros.set(nowMicros);
    }

    /**
     * Protected constructor for subclasses.
     */
    protected ClientState(boolean isInternal)
    {
        this.isInternal = isInternal;
    }

    /**
     * @return a LocalClientState object for internal C* calls (not limited by any kind of auth).
     */
    public static ClientState forInternalCalls()
    {
        return LocalClientState.forInternalCalls();
    }

    public static ClientState forInternalCalls(String keyspace)
    {
        return LocalClientState.forInternalCalls(keyspace);
    }

    /**
     * @return a LocalClientState object for external clients (native protocol users).
     */
    public static ClientState forExternalCalls(SocketAddress remoteAddress)
    {
        return LocalClientState.forExternalCalls(remoteAddress);
    }

    /**
     * Clone this ClientState object, but use the provided keyspace instead of the
     * keyspace in this ClientState object.
     *
     * @return a new LocalClientState object if the keyspace argument is non-null. Otherwise do not clone
     *   and return this ClientState object.
     */
    public ClientState cloneWithKeyspaceIfSet(String keyspace)
    {
        if (keyspace == null)
            return this;
        if (this instanceof LocalClientState)
        {
            return ((LocalClientState) this).cloneWithKeyspaceIfSet(keyspace);
        }
        throw new UnsupportedOperationException("Clone not supported for " + this.getClass().getSimpleName());
    }

    /**
     * This clock guarantees that updates for the same ClientState will be ordered
     * in the sequence seen, even if multiple updates happen in the same millisecond.
     */
    public static long getTimestamp()
    {
        while (true)
        {
            long current = currentTimeMillis() * 1000;
            long last = lastTimestampMicros.get();
            long tstamp = last >= current ? last + 1 : current;
            if (lastTimestampMicros.compareAndSet(last, tstamp))
                return tstamp;
        }
    }

    /**
     * Returns a timestamp suitable for paxos given the timestamp of the last known commit (or in progress update).
     * <p>
     * Paxos ensures that the timestamp it uses for commits respects the serial order of those commits. It does so
     * by having each replica reject any proposal whose timestamp is not strictly greater than the last proposal it
     * accepted. So in practice, which timestamp we use for a given proposal doesn't affect correctness but it does
     * affect the chance of making progress (if we pick a timestamp lower than what has been proposed before, our
     * new proposal will just get rejected).
     * <p>
     * As during the prepared phase replica send us the last propose they accepted, a first option would be to take
     * the maximum of those last accepted proposal timestamp plus 1 (and use a default value, say 0, if it's the
     * first known proposal for the partition). This would most work (giving commits the timestamp 0, 1, 2, ...
     * in the order they are commited) up to 2 important caveats:
     *   1) it would give a very poor experience when Paxos and non-Paxos updates are mixed in the same partition,
     *      since paxos operations wouldn't be using microseconds timestamps. And while you shouldn't theoretically
     *      mix the 2 kind of operations, this would still be pretty unintuitive. And what if you started writing
     *      normal updates and realize later you should switch to Paxos to enforce a property you want?
     *   2) this wouldn't actually be safe due to the expiration set on the Paxos state table.
     * <p>
     * So instead, we initially chose to use the current time in microseconds as for normal update. Which works in
     * general but mean that clock skew creates unavailability periods for Paxos updates (either a node has his clock
     * in the past and he may no be able to get commit accepted until its clock catch up, or a node has his clock in
     * the future and then once one of its commit his accepted, other nodes ones won't be until they catch up). This
     * is ok for small clock skew (few ms) but can be pretty bad for large one.
     * <p>
     * Hence our current solution: we mix both approaches. That is, we compare the timestamp of the last known
     * accepted proposal and the local time. If the local time is greater, we use it, thus keeping paxos timestamps
     * locked to the current time in general (making mixing Paxos and non-Paxos more friendly, and behaving correctly
     * when the paxos state expire (as long as your maximum clock skew is lower than the Paxos state expiration
     * time)). Otherwise (the local time is lower than the last proposal, meaning that this last proposal was done
     * with a clock in the future compared to the local one), we use the last proposal timestamp plus 1, ensuring
     * progress.
     *
     * @param minUnixMicros the max timestamp of the last proposal accepted by replica having responded
     * to the prepare phase of the paxos round this is for. In practice, that's the minimum timestamp this method
     * may return.
     * @return a timestamp suitable for a Paxos proposal (using the reasoning described above). Note that
     * contrarily to the {@link #getTimestamp()} method, the return value is not guaranteed to be unique (nor
     * monotonic) across calls since it can return it's argument (so if the same argument is passed multiple times,
     * it may be returned multiple times). Note that we still ensure Paxos "ballot" are unique (for different
     * proposal) by (securely) randomizing the non-timestamp part of the UUID.
     */
    public static long getTimestampForPaxos(long minUnixMicros)
    {
        while (true)
        {
            long current = Math.max(currentTimeMillis() * 1000, minUnixMicros);
            long last = lastTimestampMicros.get();
            long tstamp = last >= current ? last + 1 : current;
            // Note that if we ended up picking minTimestampMicrosToUse (it was "in the future"), we don't
            // want to change the local clock, otherwise a single node in the future could corrupt the clock
            // of all nodes and for all inserts (since non-paxos inserts also use lastTimestampMicros).
            // See CASSANDRA-11991
            if (tstamp == minUnixMicros || lastTimestampMicros.compareAndSet(last, tstamp))
                return tstamp;
        }
    }

    public static long getLastTimestampMicros()
    {
        return lastTimestampMicros.get();
    }

    public abstract Optional<String> getDriverName();

    public abstract Optional<String> getDriverVersion();

    public abstract Optional<Map<String,String>> getClientOptions();

    public abstract void setDriverName(String driverName);

    public abstract void setDriverVersion(String driverVersion);
    
    public abstract void setClientOptions(Map<String,String> clientOptions);

    public static QueryHandler getCQLQueryHandler()
    {
        return cqlQueryHandler;
    }

    public abstract InetSocketAddress getRemoteAddress();

    public abstract InetAddress getClientAddress();

    public String getRawKeyspace()
    {
        return keyspace;
    }

    /**
     * Protected method for subclasses to set the keyspace.
     */
    protected void setRawKeyspace(String keyspace)
    {
        this.keyspace = keyspace;
    }

    public String getKeyspace() throws InvalidRequestException
    {
        if (keyspace == null)
            throw new InvalidRequestException("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
        return keyspace;
    }

    public abstract void setKeyspace(String ks);

    /**
     * Attempts to login the given user.
     */
    public abstract void login(AuthenticatedUser user);

    public void ensureAllKeyspacesPermission(Permission perm)
    {
        if (isInternal)
            return;
        validateLogin();
        ensurePermission(perm, DataResource.root());
    }

    public void ensureKeyspacePermission(String keyspace, Permission perm)
    {
        ensurePermission(keyspace, perm, DataResource.keyspace(keyspace));
    }

    public void ensureAllTablesPermission(String keyspace, Permission perm)
    {
        ensurePermission(keyspace, perm, DataResource.allTables(keyspace));
    }

    public void ensureTablePermission(String keyspace, String table, Permission perm)
    {
        ensurePermission(keyspace, perm, DataResource.table(keyspace, table));
    }

    public void ensureTablePermission(TableMetadataRef tableRef, Permission perm)
    {
        ensureTablePermission(tableRef.get(), perm);
    }

    public void ensureTablePermission(TableMetadata table, Permission perm)
    {
        ensurePermission(table.keyspace, perm, table.resource);
    }

    public abstract boolean hasTablePermission(TableMetadata table, Permission perm);

    private void ensurePermission(String keyspace, Permission perm, DataResource resource)
    {
        validateKeyspace(keyspace);

        if (isInternal)
            return;

        validateLogin();

        preventSystemKSSchemaModification(keyspace, resource, perm);

        // Some system data is always readable
        if ((perm == Permission.SELECT) && READABLE_SYSTEM_RESOURCES.contains(resource))
            return;

        // Modifications to any resource upon which the authenticator, authorizer or role manager depend should not be
        // be performed by users
        if (PROTECTED_AUTH_RESOURCES.contains(resource))
            if ((perm == Permission.CREATE) || (perm == Permission.ALTER) || (perm == Permission.DROP))
                throw new UnauthorizedException(String.format("%s schema is protected", resource));

        ensurePermission(perm, resource);
    }

    public abstract void ensurePermission(Permission perm, IResource resource);

    // Convenience method called from authorize method of CQLStatement
    // Also avoids needlessly creating lots of FunctionResource objects
    public abstract void ensurePermission(Permission permission, Function function);

    private void preventSystemKSSchemaModification(String keyspace, DataResource resource, Permission perm)
    {
        // we only care about DDL statements
        if (perm != Permission.ALTER && perm != Permission.DROP && perm != Permission.CREATE)
            return;

        // prevent ALL local system keyspace modification
        if (SchemaConstants.isLocalSystemKeyspace(keyspace))
            throw new UnauthorizedException(keyspace + " keyspace is not user-modifiable.");

        if (SchemaConstants.isReplicatedSystemKeyspace(keyspace))
        {
            // allow users with sufficient privileges to alter replication params of replicated system keyspaces
            if (perm == Permission.ALTER && resource.isKeyspaceLevel())
                return;

            // prevent all other modifications of replicated system keyspaces
            throw new UnauthorizedException(String.format("Cannot %s %s", perm, resource));
        }
    }

    public abstract void validateLogin();

    public abstract void ensureNotAnonymous();

    /**
     * Checks if this user is an ordinary user (not a super or system user).
     *
     * @return {@code true} if this user is an ordinary user, {@code false} otherwise.
     */
    public boolean isOrdinaryUser()
    {
        return !isSuper() && !isSystem();
    }

    /**
     * Checks if this user is a super user.
     */
    public abstract boolean isSuper();

    /**
     * Checks if the user is the system user.
     *
     * @return {@code true} if this user is the system user, {@code false} otherwise.
     */
    public boolean isSystem()
    {
        return isInternal;
    }

    public void ensureIsSuperuser(String message)
    {
        if (!isSuper())
            throw new UnauthorizedException(message);
    }

    public abstract void warnAboutUseWithPreparedStatements(MD5Digest statementId, String preparedKeyspace);

    public abstract void warnAboutUneligiblePreparedStatement(MD5Digest statementId);

    private static void validateKeyspace(String keyspace)
    {
        if (keyspace == null)
            throw new InvalidRequestException("You have not set a keyspace for this session");
    }

    public abstract AuthenticatedUser getUser();

    protected abstract Set<Permission> authorize(IResource resource);

}
