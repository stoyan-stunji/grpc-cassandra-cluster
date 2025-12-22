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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.Resources;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.dht.Datacenters;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.MD5Digest;

/**
 * Local client state implementation that maintains full AuthenticatedUser functionality.
 * Used for local operations where the complete user object and permissions are available.
 */
public class LocalClientState extends ClientState
{
    // User-dependent fields
    private volatile AuthenticatedUser user;
    private final InetSocketAddress remoteAddress;

    // Warning state (local sessions only)
    private volatile boolean issuedPreparedStatementsUseWarning;
    private volatile boolean issuedWarningForUneligiblePreparedStatements;

    // Client information (local sessions only)
    private volatile String driverName;
    private volatile String driverVersion;
    private volatile Map<String,String> clientOptions;

    /**
     * Constructor for internal calls.
     */
    private LocalClientState()
    {
        super(true); // isInternal = true
        this.remoteAddress = null;
    }

    /**
     * Constructor for external clients.
     */
    public LocalClientState(InetSocketAddress remoteAddress)
    {
        super(false); // isInternal = false
        this.remoteAddress = remoteAddress;
        if (!DatabaseDescriptor.getAuthenticator().requireAuthentication())
            this.user = AuthenticatedUser.ANONYMOUS_USER;
    }

    /**
     * Copy constructor.
     */
    public LocalClientState(LocalClientState source)
    {
        super(source.isInternal);
        this.remoteAddress = source.remoteAddress;
        this.user = source.user;
        this.setRawKeyspace(source.getRawKeyspace());
        this.driverName = source.driverName;
        this.driverVersion = source.driverVersion;
        this.clientOptions = source.clientOptions;
    }

    // Factory methods
    public static LocalClientState forInternalCalls()
    {
        return new LocalClientState();
    }

    public static LocalClientState forInternalCalls(String keyspace)
    {
        LocalClientState state = new LocalClientState();
        state.setKeyspace(keyspace);
        return state;
    }

    public static LocalClientState forExternalCalls(SocketAddress remoteAddress)
    {
        return new LocalClientState((InetSocketAddress)remoteAddress);
    }

    public LocalClientState cloneWithKeyspaceIfSet(String keyspace)
    {
        if (keyspace == null)
            return this;
        LocalClientState clientState = new LocalClientState(this);
        clientState.setKeyspace(keyspace);
        return clientState;
    }

    // Abstract method implementations

    @Override
    public AuthenticatedUser getUser()
    {
        return user;
    }

    @Override
    public boolean isSuper()
    {
        return !DatabaseDescriptor.getAuthenticator().requireAuthentication() || (user != null && user.isSuper());
    }

    @Override
    public void login(AuthenticatedUser user)
    {
        if (user.isAnonymous() || canLogin(user))
            this.user = user;
        else
            throw new AuthenticationException(String.format("%s is not permitted to log in", user.getName()));
    }

    private boolean canLogin(AuthenticatedUser user)
    {
        try
        {
            return user.canLogin();
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            throw new AuthenticationException("Unable to perform authentication: " + e.getMessage(), e);
        }
    }

    @Override
    public void setKeyspace(String ks)
    {
        // Skip keyspace validation for non-authenticated users. Apparently, some client libraries
        // call set_keyspace() before calling login(), and we have to handle that.
        if (user != null && Schema.instance.getKeyspaceMetadata(ks) == null)
            throw new InvalidRequestException("Keyspace '" + ks + "' does not exist");
        setRawKeyspace(ks);
    }

    @Override
    protected Set<Permission> authorize(IResource resource)
    {
        return user.getPermissions(resource);
    }

    @Override
    public void ensurePermission(Permission perm, IResource resource)
    {
        if (!DatabaseDescriptor.getAuthorizer().requireAuthorization())
            return;

        // Access to built in functions is unrestricted
        if(resource instanceof org.apache.cassandra.auth.FunctionResource && resource.hasParent())
            if (((org.apache.cassandra.auth.FunctionResource)resource).getKeyspace().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME))
                return;

        if (resource instanceof org.apache.cassandra.auth.DataResource && isOrdinaryUser())
        {
            org.apache.cassandra.auth.DataResource dataResource = (org.apache.cassandra.auth.DataResource)resource;
            if (!dataResource.isRootLevel())
            {
                String keyspace = dataResource.getKeyspace();
                // A user may have permissions granted on ALL KEYSPACES, but this should exclude system keyspaces. Any
                // permission on those keyspaces or their tables must be granted to the user either explicitly or
                // transitively. The set of grantable permissions for non-virtual system keyspaces is further limited,
                // see the Permission enum for details.
                if (SchemaConstants.isSystemKeyspace(keyspace))
                {
                    ensurePermissionOnResourceChain(perm, Resources.chain(dataResource, IResource::hasParent));
                    return;
                }
            }
        }

        ensurePermissionOnResourceChain(perm, resource);
    }

    @Override
    public void ensurePermission(Permission permission, Function function)
    {
        // Save creating a FunctionResource is we don't need to
        if (!DatabaseDescriptor.getAuthorizer().requireAuthorization())
            return;

        // built in functions are always available to all
        if (function.isNative())
            return;

        ensurePermissionOnResourceChain(permission, org.apache.cassandra.auth.FunctionResource.function(function.name().keyspace,
                                                                              function.name().name,
                                                                              function.argTypes()));
    }

    private void ensurePermissionOnResourceChain(Permission perm, IResource resource)
    {
        ensurePermissionOnResourceChain(perm, Resources.chain(resource));
    }

    private void ensurePermissionOnResourceChain(Permission perm, List<? extends IResource> resources)
    {
        IResource resource = resources.get(0);
        if (DatabaseDescriptor.getAuthFromRoot())
            resources = Lists.reverse(resources);

        for (IResource r : resources)
            if (authorize(r).contains(perm))
                return;

        throw new UnauthorizedException(String.format("User %s has no %s permission on %s or any of its parents",
                                                      user.getName(),
                                                      perm,
                                                      resource));
    }

    @Override
    public boolean hasTablePermission(TableMetadata table, Permission perm)
    {
        if (isInternal)
            return true;

        validateLogin();

        if (!DatabaseDescriptor.getAuthorizer().requireAuthorization())
            return true;

        List<? extends IResource> resources = Resources.chain(table.resource);
        if (DatabaseDescriptor.getAuthFromRoot())
            resources = Lists.reverse(resources);

        for (IResource r : resources)
            if (authorize(r).contains(perm))
                return true;

        return false;
    }

    @Override
    public void validateLogin()
    {
        if (user == null)
        {
            throw new UnauthorizedException("You have not logged in");
        }
        else if (!user.hasLocalAccess())
        {
            throw new UnauthorizedException(String.format("You do not have access to this datacenter (%s)", Datacenters.thisDatacenter()));
        }
        else
        {
            if (remoteAddress != null && !user.hasAccessFromIp(remoteAddress))
                throw new UnauthorizedException("You do not have access from this IP " + remoteAddress.getHostString());
        }
    }

    @Override
    public void ensureNotAnonymous()
    {
        validateLogin();
        if (user.isAnonymous())
            throw new UnauthorizedException("You have to be logged in and not anonymous to perform this request");
    }

    @Override
    public InetSocketAddress getRemoteAddress()
    {
        return remoteAddress;
    }

    @Override
    public InetAddress getClientAddress()
    {
        return isInternal ? null : remoteAddress.getAddress();
    }

    @Override
    public Optional<String> getDriverName()
    {
        return Optional.ofNullable(driverName);
    }

    @Override
    public Optional<String> getDriverVersion()
    {
        return Optional.ofNullable(driverVersion);
    }

    @Override
    public Optional<Map<String,String>> getClientOptions()
    {
        return Optional.ofNullable(clientOptions);
    }

    @Override
    public void setDriverName(String driverName)
    {
        this.driverName = driverName;
    }

    @Override
    public void setDriverVersion(String driverVersion)
    {
        this.driverVersion = driverVersion;
    }

    @Override
    public void setClientOptions(Map<String,String> clientOptions)
    {
        this.clientOptions = ImmutableMap.copyOf(clientOptions);
    }

    @Override
    public void warnAboutUseWithPreparedStatements(MD5Digest statementId, String preparedKeyspace)
    {
        if (!issuedPreparedStatementsUseWarning)
        {
            ClientWarn.instance.warn(String.format("`USE <keyspace>` with prepared statements is considered to be an anti-pattern due to ambiguity in non-qualified table names. " +
                                                   "Please consider removing instances of `Session#setKeyspace(<keyspace>)`, `Session#execute(\"USE <keyspace>\")` and `cluster.newSession(<keyspace>)` from your code, and " +
                                                   "always use fully qualified table names (e.g. <keyspace>.<table>). " +
                                                   "Keyspace used: %s, statement keyspace: %s, statement id: %s", getRawKeyspace(), preparedKeyspace, statementId));
            issuedPreparedStatementsUseWarning = true;
        }
    }

    @Override
    public void warnAboutUneligiblePreparedStatement(MD5Digest statementId)
    {
        if (!issuedWarningForUneligiblePreparedStatements)
        {
            ClientWarn.instance.warn(String.format("Prepared statements for other than modification and selection statements should be avoided, statement id: %s", statementId));
            issuedWarningForUneligiblePreparedStatements = true;
        }
    }
}