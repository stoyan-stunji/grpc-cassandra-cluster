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

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

public class WriteFailureException extends RequestFailureException
{
    public final WriteType writeType;

    public WriteFailureException(ConsistencyLevel consistency, int received, int blockFor, WriteType writeType, Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint)
    {
        super(ExceptionCode.WRITE_FAILURE, consistency, received, blockFor, ImmutableMap.copyOf(failureReasonByEndpoint));
        this.writeType = writeType;
    }

    @Override
    protected void serializeSpecificFields(DataOutputPlus out, int version) throws IOException
    {
        out.writeByte(consistency.code);
        out.writeInt(received);
        out.writeInt(blockFor);

        // Serialize failure reason map
        out.writeInt(failureReasonByEndpoint.size());
        for (Map.Entry<InetAddressAndPort, RequestFailureReason> entry : failureReasonByEndpoint.entrySet())
        {
            InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(entry.getKey(), out, version);
            out.writeShort(entry.getValue().code);
        }

        out.writeUTF(writeType.toString());
    }

    @Override
    protected long serializedSizeSpecificFields(int version)
    {
        long size = TypeSizes.BYTE_SIZE + // consistency
                    TypeSizes.INT_SIZE +   // received
                    TypeSizes.INT_SIZE +   // blockFor
                    TypeSizes.INT_SIZE;    // map size

        for (Map.Entry<InetAddressAndPort, RequestFailureReason> entry : failureReasonByEndpoint.entrySet())
        {
            size += InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(entry.getKey(), version);
            size += TypeSizes.SHORT_SIZE; // reason code
        }

        size += TypeSizes.sizeof(writeType.toString());
        return size;
    }

    static WriteFailureException deserializeFields(String message, DataInputPlus in, int version) throws IOException
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

        WriteType writeType = WriteType.valueOf(in.readUTF());
        return new WriteFailureException(consistency, received, blockFor, writeType, failures);
    }

    @Override
    public CassandraExceptionCode getCassandraExceptionCode()
    {
        return CassandraExceptionCode.WRITE_FAILURE;
    }
}
