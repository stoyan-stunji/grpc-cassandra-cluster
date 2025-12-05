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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.MD5Digest;

/**
 * A serializable version of ClientState designed for forwarding operations to remote coordinators.
 *
 * This class contains only the essential authorization attributes needed for forwarded operations,
 * avoiding the complex serialization challenges of the full AuthenticatedUser object.
 *
 * Key design principles:
 * - Contains only serializable authorization state (isSuper, isInternal, etc.)
 * - Supports guardrail enforcement through isOrdinaryUser() and applyGuardrails()
 * - Throws exceptions for operations requiring the full user object (getUser(), authorize())
 * - Used specifically for CAS and consensus read forwarding in tracked keyspaces
 */
public class RemoteClientState extends ClientState
{
    public static final Serializer serializer = new Serializer();

    // Essential serializable state for authorization context
    private final boolean isSuperCached;
    private final String userName; // For logging/error messages (optional)

    /**
     * Creates a RemoteClientState from a local ClientState for forwarding.
     * Extracts only the essential authorization attributes.
     */
    public static RemoteClientState from(ClientState localState)
    {
        if (!(localState instanceof LocalClientState))
        {
            throw new IllegalArgumentException("Cannot create RemoteClientState from non-LocalClientState: " + localState.getClass());
        }
        LocalClientState local = (LocalClientState) localState;

        return new RemoteClientState(
            localState.isSuper(),
            localState.isSystem(),
            localState.applyGuardrails(),
            localState.getRawKeyspace(),
            local.getUser() != null ? local.getUser().getName() : null
        );
    }

    /**
     * Private constructor for deserialization and from() method.
     */
    private RemoteClientState(boolean isSuper, boolean isInternal, boolean applyGuardrails, String keyspace, String userName)
    {
        super(isInternal);
        this.isSuperCached = isSuper;
        this.userName = userName;

        // Set the cached guardrail and keyspace state in the base class
        if (!applyGuardrails)
            pauseGuardrails();
        if (keyspace != null)
            setRawKeyspace(keyspace);
    }

    // ==========================================
    // Cached user attributes (implemented with cached values)
    // ==========================================

    @Override
    public boolean isSuper()
    {
        return isSuperCached;
    }

    // ==========================================
    // Methods that throw exceptions for remote operations
    // ==========================================

    @Override
    public AuthenticatedUser getUser()
    {
        throw new UnsupportedOperationException("Cannot access user object in remote client state. " +
                                              "Full user operations must be performed on the original coordinator.");
    }

    @Override
    public void login(AuthenticatedUser user)
    {
        throw new UnsupportedOperationException("Cannot login on remote client state.");
    }

    @Override
    public void setKeyspace(String ks)
    {
        throw new UnsupportedOperationException("Cannot modify keyspace on remote client state.");
    }

    @Override
    protected Set<Permission> authorize(IResource resource)
    {
        throw new UnsupportedOperationException("Cannot perform authorization in remote client state. " +
                                              "Authorization must be performed on the original coordinator before forwarding.");
    }

    @Override
    public void ensurePermission(Permission perm, IResource resource)
    {
        throw new UnsupportedOperationException("Cannot perform authorization in remote client state. " +
                                              "Authorization must be performed on the original coordinator before forwarding.");
    }

    @Override
    public void ensurePermission(Permission permission, Function function)
    {
        throw new UnsupportedOperationException("Cannot perform authorization in remote client state. " +
                                              "Authorization must be performed on the original coordinator before forwarding.");
    }

    @Override
    public boolean hasTablePermission(TableMetadata table, Permission perm)
    {
        throw new UnsupportedOperationException("Cannot check table permissions in remote client state. " +
                                              "Permission checks must be performed on the original coordinator before forwarding.");
    }

    @Override
    public void validateLogin()
    {
        throw new UnsupportedOperationException("Cannot validate login in remote client state. " +
                                              "Login validation must be performed on the original coordinator before forwarding.");
    }

    @Override
    public void ensureNotAnonymous()
    {
        throw new UnsupportedOperationException("Cannot validate anonymous status in remote client state. " +
                                              "Anonymous validation must be performed on the original coordinator before forwarding.");
    }

    @Override
    public InetSocketAddress getRemoteAddress()
    {
        throw new UnsupportedOperationException("Remote address not available in remote client state.");
    }

    @Override
    public InetAddress getClientAddress()
    {
        throw new UnsupportedOperationException("Client address not available in remote client state.");
    }

    @Override
    public Optional<String> getDriverName()
    {
        throw new UnsupportedOperationException("Driver information not available in remote client state.");
    }

    @Override
    public Optional<String> getDriverVersion()
    {
        throw new UnsupportedOperationException("Driver information not available in remote client state.");
    }

    @Override
    public Optional<Map<String, String>> getClientOptions()
    {
        throw new UnsupportedOperationException("Client options not available in remote client state.");
    }

    @Override
    public void setDriverName(String driverName)
    {
        throw new UnsupportedOperationException("Cannot modify driver information in remote client state.");
    }

    @Override
    public void setDriverVersion(String driverVersion)
    {
        throw new UnsupportedOperationException("Cannot modify driver information in remote client state.");
    }

    @Override
    public void setClientOptions(Map<String, String> clientOptions)
    {
        throw new UnsupportedOperationException("Cannot modify client options in remote client state.");
    }

    @Override
    public void warnAboutUseWithPreparedStatements(MD5Digest statementId, String preparedKeyspace)
    {
        throw new UnsupportedOperationException("Cannot issue warnings in remote client state. " +
                                              "Warnings should be issued on the original coordinator.");
    }

    @Override
    public void warnAboutUneligiblePreparedStatement(MD5Digest statementId)
    {
        throw new UnsupportedOperationException("Cannot issue warnings in remote client state. " +
                                              "Warnings should be issued on the original coordinator.");
    }

    /**
     * Get the user name for logging purposes (if available).
     */
    public String getUserNameForLogging()
    {
        return userName != null ? userName : "unknown";
    }

    /**
     * Serializer for RemoteClientState to enable network transmission.
     */
    public static class Serializer implements IVersionedSerializer<RemoteClientState>
    {
        @Override
        public void serialize(RemoteClientState state, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(state.isSuperCached);
            out.writeBoolean(state.isInternal);
            out.writeBoolean(state.applyGuardrails());

            // Serialize keyspace (nullable)
            String keyspace = state.getRawKeyspace();
            if (keyspace != null)
            {
                out.writeBoolean(true);
                out.writeUTF(keyspace);
            }
            else
            {
                out.writeBoolean(false);
            }

            // Serialize user name (nullable)
            if (state.userName != null)
            {
                out.writeBoolean(true);
                out.writeUTF(state.userName);
            }
            else
            {
                out.writeBoolean(false);
            }
        }

        @Override
        public RemoteClientState deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean isSuper = in.readBoolean();
            boolean isInternal = in.readBoolean();
            boolean applyGuardrails = in.readBoolean();

            String keyspace = null;
            boolean hasKeyspace = in.readBoolean();
            if (hasKeyspace)
            {
                keyspace = in.readUTF();
            }

            String userName = null;
            boolean hasUserName = in.readBoolean();
            if (hasUserName)
            {
                userName = in.readUTF();
            }

            return new RemoteClientState(isSuper, isInternal, applyGuardrails, keyspace, userName);
        }

        @Override
        public long serializedSize(RemoteClientState state, int version)
        {
            long size = 0;
            size += 3; // Three boolean flags
            size += 1; // hasKeyspace flag
            size += 1; // hasUserName flag

            String keyspace = state.getRawKeyspace();
            if (keyspace != null)
            {
                size += TypeSizes.sizeof(keyspace);
            }

            if (state.userName != null)
            {
                size += TypeSizes.sizeof(state.userName);
            }

            return size;
        }
    }
}