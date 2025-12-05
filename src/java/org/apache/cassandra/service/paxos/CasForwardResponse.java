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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.db.SerializationHeader.StableHeaderSerializer.STABLE;
import static org.apache.cassandra.db.rows.DeserializationHelper.Flag.FROM_REMOTE;

/**
 * Response containing the result of a forwarded CAS operation.
 * Can contain either a successful result or an exception that occurred during execution.
 */
public class CasForwardResponse
{
    public static final Serializer serializer = new Serializer();

    public final RowIterator result;
    public final CassandraException exception;
    public final List<String> warnings;

    public CasForwardResponse(RowIterator result, List<String> warnings)
    {
        this.result = result;
        this.exception = null;
        this.warnings = warnings;
    }

    public CasForwardResponse(CassandraException exception, List<String> warnings)
    {
        this.result = null;
        this.exception = exception;
        this.warnings = warnings;
    }

    public boolean isSuccess()
    {
        return exception == null;
    }

    public static class Serializer implements IVersionedSerializer<CasForwardResponse>
    {
        @Override
        public void serialize(CasForwardResponse response, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(response.exception != null);
            if (response.exception != null)
            {
                CassandraException.serializer.serialize(response.exception, out, version);
            }
            else
            {
                if (response.result == null)
                {
                    out.writeBoolean(false); // no result data
                }
                else
                {
                    out.writeBoolean(true); // has result data

                    // Use exact pattern from TxnDataKeyValue.java
                    FilteredPartition partition = new FilteredPartition(response.result);
                    partition.metadata().id.serializeCompact(out);
                    try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
                    {
                        UnfilteredRowIteratorSerializer.serializer.serialize(iterator, out, version, partition.rowCount(), STABLE, null);
                    }
                }
            }

            // Serialize warnings
            if (response.warnings == null || response.warnings.isEmpty())
            {
                out.writeInt(0);
            }
            else
            {
                out.writeInt(response.warnings.size());
                for (String warning : response.warnings)
                {
                    out.writeUTF(warning);
                }
            }
        }

        @Override
        public CasForwardResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean hasException = in.readBoolean();
            RowIterator result = null;
            CassandraException exception = null;

            if (hasException)
            {
                exception = CassandraException.serializer.deserialize(in, version);
            }
            else
            {
                boolean hasResult = in.readBoolean();
                if (hasResult)
                {
                    // Use exact pattern from TxnDataKeyValue.java
                    TableMetadata metadata = Schema.instance.getExistingTableMetadata(TableId.deserializeCompact(in));
                    UnfilteredRowIteratorSerializer.Header header = UnfilteredRowIteratorSerializer.serializer.deserializeHeader(metadata, in, version, FROM_REMOTE, STABLE, null);
                    try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, metadata, FROM_REMOTE, header))
                    {
                        result = UnfilteredRowIterators.filter(partition, 0);
                    }
                }
            }

            // Deserialize warnings
            int warningCount = in.readInt();
            List<String> warnings = null;
            if (warningCount > 0)
            {
                warnings = new ArrayList<>(warningCount);
                for (int i = 0; i < warningCount; i++)
                {
                    warnings.add(in.readUTF());
                }
            }

            if (hasException)
                return new CasForwardResponse(exception, warnings);
            else
                return new CasForwardResponse(result, warnings);
        }

        @Override
        public long serializedSize(CasForwardResponse response, int version)
        {
            long size = TypeSizes.BOOL_SIZE; // hasException flag
            if (response.exception != null)
            {
                size += CassandraException.serializer.serializedSize(response.exception, version);
            }
            else
            {
                size += TypeSizes.BOOL_SIZE; // hasResult flag
                if (response.result != null)
                {
                    // Use exact pattern from TxnDataKeyValue.java
                    FilteredPartition partition = new FilteredPartition(response.result);
                    TableId tableId = partition.metadata().id;
                    size += tableId.serializedCompactSize();
                    try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
                    {
                        size += UnfilteredRowIteratorSerializer.serializer.serializedSize(iterator, version, partition.rowCount(), STABLE, null);
                    }
                }
            }

            // Warnings size
            size += TypeSizes.INT_SIZE; // warning count
            if (response.warnings != null)
            {
                for (String warning : response.warnings)
                {
                    size += TypeSizes.sizeof(warning);
                }
            }
            return size;
        }
    }
}