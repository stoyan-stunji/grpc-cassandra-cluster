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

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class WriteTimeoutException extends RequestTimeoutException
{
    public final WriteType writeType;

    public WriteTimeoutException(WriteType writeType, ConsistencyLevel consistency, int received, int blockFor)
    {
        super(ExceptionCode.WRITE_TIMEOUT, consistency, received, blockFor,
              String.format("Operation timed out - received %d/%d responses.", received, blockFor));
        this.writeType = writeType;
    }

    public WriteTimeoutException(WriteType writeType, ConsistencyLevel consistency, int received, int blockFor, String msg)
    {
        super(ExceptionCode.WRITE_TIMEOUT, consistency, received, blockFor, msg);
        this.writeType = writeType;
    }

    @Override
    protected void serializeSpecificFields(DataOutputPlus out, int version) throws IOException
    {
        out.writeByte(consistency.code);
        out.writeInt(received);
        out.writeInt(blockFor);
        out.writeUTF(writeType.toString());
    }

    @Override
    protected long serializedSizeSpecificFields(int version)
    {
        return TypeSizes.BYTE_SIZE + // consistency
               TypeSizes.INT_SIZE +   // received
               TypeSizes.INT_SIZE +   // blockFor
               TypeSizes.sizeof(writeType.toString());
    }

    static WriteTimeoutException deserializeFields(String message, DataInputPlus in, int version) throws IOException
    {
        ConsistencyLevel consistency = ConsistencyLevel.fromCode(in.readUnsignedByte());
        int received = in.readInt();
        int blockFor = in.readInt();
        WriteType writeType = WriteType.valueOf(in.readUTF());

        return new WriteTimeoutException(writeType, consistency, received, blockFor, message);
    }

    @Override
    public CassandraExceptionCode getCassandraExceptionCode()
    {
        return CassandraExceptionCode.WRITE_TIMEOUT;
    }
}
