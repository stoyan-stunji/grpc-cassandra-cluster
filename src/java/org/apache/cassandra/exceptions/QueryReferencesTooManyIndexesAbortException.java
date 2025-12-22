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

package org.apache.cassandra.exceptions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

public class QueryReferencesTooManyIndexesAbortException extends ReadAbortException
{
    public final int nodes;
    public final long maxValue;

    public QueryReferencesTooManyIndexesAbortException(String msg, int nodes, long maxValue, boolean dataPresent, ConsistencyLevel consistency, int received, int blockFor, Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint)
    {
        super(msg, consistency, received, blockFor, dataPresent, failureReasonByEndpoint);
        this.nodes = nodes;
        this.maxValue = maxValue;
    }

    @Override
    protected void serializeSpecificFields(DataOutputPlus out, int version) throws IOException
    {
        // Serialize parent fields first
        super.serializeSpecificFields(out, version);
        // Add QueryReferencesTooManyIndexesAbortException specific fields
        out.writeInt(nodes);
        out.writeLong(maxValue);
    }

    @Override
    protected long serializedSizeSpecificFields(int version)
    {
        return super.serializedSizeSpecificFields(version) +
               TypeSizes.INT_SIZE +  // nodes
               TypeSizes.LONG_SIZE;  // maxValue
    }

    static QueryReferencesTooManyIndexesAbortException deserializeFields(String message, DataInputPlus in, int version) throws IOException
    {
        ConsistencyLevel consistency = ConsistencyLevel.fromCode(in.readUnsignedByte());
        int received = in.readInt();
        int blockFor = in.readInt();

        // Deserialize failure reason map
        int mapSize = in.readInt();
        Map<InetAddressAndPort, RequestFailureReason> failures = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++)
        {
            InetAddressAndPort endpoint = InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version);
            RequestFailureReason reason = RequestFailureReason.fromCode(in.readShort());
            failures.put(endpoint, reason);
        }

        boolean dataPresent = in.readBoolean();
        int nodes = in.readInt();
        long maxValue = in.readLong();

        return new QueryReferencesTooManyIndexesAbortException(message, nodes, maxValue, dataPresent, consistency, received, blockFor, failures);
    }

    @Override
    public CassandraExceptionCode getCassandraExceptionCode()
    {
        return CassandraExceptionCode.QUERY_TOO_MANY_INDEXES_ABORT;
    }
}
