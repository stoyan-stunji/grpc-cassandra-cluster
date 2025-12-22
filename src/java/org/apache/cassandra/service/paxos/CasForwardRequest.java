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

package org.apache.cassandra.service.paxos;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.RemoteClientState;

/**
 * Request to forward a CAS operation to a replica coordinator for tracked keyspaces.
 * Contains the essential information needed to execute the CAS operation on the remote coordinator.
 */
public class CasForwardRequest
{
    public static final Serializer serializer = new Serializer();

    public final String keyspaceName;
    public final String cfName;
    public final DecoratedKey key;
    public final ConsistencyLevel consistencyForPaxos;
    public final ConsistencyLevel consistencyForCommit;
    public final long nowInSeconds;
    public final RemoteClientState clientState;
    public final CQL3CasRequest casRequest;  // The actual CAS request to forward

    /**
     * Backward compatibility constructor that accepts ClientState.
     */
    public CasForwardRequest(String keyspaceName,
                            String cfName,
                            DecoratedKey key,
                            CQL3CasRequest request,
                            ConsistencyLevel consistencyForPaxos,
                            ConsistencyLevel consistencyForCommit,
                            ClientState clientState,
                            long nowInSeconds)
    {
        Preconditions.checkNotNull(keyspaceName, "keyspaceName cannot be null");
        Preconditions.checkNotNull(cfName, "cfName cannot be null");
        Preconditions.checkNotNull(key, "key cannot be null");
        Preconditions.checkNotNull(request, "request cannot be null");
        Preconditions.checkNotNull(consistencyForPaxos, "consistencyForPaxos cannot be null");
        Preconditions.checkNotNull(consistencyForCommit, "consistencyForCommit cannot be null");
        Preconditions.checkNotNull(clientState, "clientState cannot be null");
        this.keyspaceName = keyspaceName;
        this.cfName = cfName;
        this.key = key;
        this.consistencyForPaxos = consistencyForPaxos;
        this.consistencyForCommit = consistencyForCommit;
        this.nowInSeconds = nowInSeconds;
        this.clientState = RemoteClientState.from(clientState);
        this.casRequest = request;
    }

    /**
     * Special constructor for deserialization that accepts RemoteClientState directly.
     */
    private CasForwardRequest(String keyspaceName,
                             String cfName,
                             DecoratedKey key,
                             RemoteClientState clientState,
                             ConsistencyLevel consistencyForPaxos,
                             ConsistencyLevel consistencyForCommit,
                             long nowInSeconds,
                             CQL3CasRequest casRequest)
    {
        this.keyspaceName = keyspaceName;
        this.cfName = cfName;
        this.key = key;
        this.clientState = clientState;
        this.consistencyForPaxos = consistencyForPaxos;
        this.consistencyForCommit = consistencyForCommit;
        this.nowInSeconds = nowInSeconds;
        this.casRequest = casRequest;
    }

    public static class Serializer implements IVersionedSerializer<CasForwardRequest>
    {
        @Override
        public void serialize(CasForwardRequest forwardRequest, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(forwardRequest.keyspaceName);
            out.writeUTF(forwardRequest.cfName);
            DecoratedKey.serializer.serialize(forwardRequest.key, out, version);
            out.writeByte(forwardRequest.consistencyForPaxos.code);
            out.writeByte(forwardRequest.consistencyForCommit.code);
            out.writeLong(forwardRequest.nowInSeconds);
            RemoteClientState.serializer.serialize(forwardRequest.clientState, out, version);
            CQL3CasRequest.Serializer.instance.serialize((CQL3CasRequest) forwardRequest.casRequest, out, version);
        }

        @Override
        public CasForwardRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            String keyspaceName = in.readUTF();
            String cfName = in.readUTF();
            DecoratedKey key = (DecoratedKey) DecoratedKey.serializer.deserialize(in, version);
            ConsistencyLevel consistencyForPaxos = ConsistencyLevel.fromCode(in.readUnsignedByte());
            ConsistencyLevel consistencyForCommit = ConsistencyLevel.fromCode(in.readUnsignedByte());
            long nowInSeconds = in.readLong();
            RemoteClientState clientState = RemoteClientState.serializer.deserialize(in, version);
            CQL3CasRequest casRequest = CQL3CasRequest.Serializer.instance.deserialize(in, version);

            return new CasForwardRequest(keyspaceName, cfName, key, clientState,
                                       consistencyForPaxos, consistencyForCommit, nowInSeconds, casRequest);
        }

        @Override
        public long serializedSize(CasForwardRequest forwardRequest, int version)
        {
            long size = 0;
            size += TypeSizes.sizeof(forwardRequest.keyspaceName);
            size += TypeSizes.sizeof(forwardRequest.cfName);
            size += DecoratedKey.serializer.serializedSize(forwardRequest.key, version);
            size += 1; // consistencyForPaxos.code
            size += 1; // consistencyForCommit.code
            size += TypeSizes.LONG_SIZE; // nowInSeconds
            size += RemoteClientState.serializer.serializedSize(forwardRequest.clientState, version);
            size += CQL3CasRequest.Serializer.instance.serializedSize(forwardRequest.casRequest, version);

            return size;
        }
    }
}