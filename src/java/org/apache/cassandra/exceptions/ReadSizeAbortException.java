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
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

public class ReadSizeAbortException extends ReadAbortException
{
    public ReadSizeAbortException(String msg, ConsistencyLevel consistency, int received, int blockFor, boolean dataPresent, Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint)
    {
        super(msg, consistency, received, blockFor, dataPresent, failureReasonByEndpoint);
    }

    static ReadSizeAbortException deserializeFields(String message, DataInputPlus in, int version) throws IOException
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

        return new ReadSizeAbortException(message, consistency, received, blockFor, dataPresent, failures);
    }

    @Override
    public CassandraExceptionCode getCassandraExceptionCode()
    {
        return CassandraExceptionCode.READ_SIZE_ABORT;
    }
}
