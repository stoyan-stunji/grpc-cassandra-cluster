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

package org.apache.cassandra.io.sstable.format;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IFilter;

import static org.apache.cassandra.config.Config.DiskAccessMode;

public abstract class SortedTableReaderLoadingBuilder<R extends SSTableReader, B extends SSTableReader.Builder<R, B>>
extends SSTableReaderLoadingBuilder<R, B>
{
    private final static Logger logger = LoggerFactory.getLogger(SortedTableReaderLoadingBuilder.class);
    private FileHandle.Builder dataFileBuilder;
    private FileHandle.Builder directFileBuilder;

    public SortedTableReaderLoadingBuilder(SSTable.Builder<?, ?> builder)
    {
        super(builder);
    }

    protected IFilter loadFilter(ValidationMetadata validationMetadata)
    {
        return FilterComponent.maybeLoadBloomFilter(descriptor,
                                                    components,
                                                    tableMetadataRef.get(),
                                                    validationMetadata);
    }

    protected FileHandle.Builder dataFileBuilder(StatsMetadata statsMetadata, CompressionMetadata compressionMetadata)
    {
        assert this.dataFileBuilder == null || this.dataFileBuilder.file.equals(descriptor.fileFor(BtiFormat.Components.DATA));

        logger.info("Opening {} ({})", descriptor, FBUtilities.prettyPrintMemory(descriptor.fileFor(BtiFormat.Components.DATA).length()));

        if (dataFileBuilder == null)
            dataFileBuilder = new FileHandle.Builder(descriptor.fileFor(BtiFormat.Components.DATA));

        return dataFileBuilder(dataFileBuilder, statsMetadata, compressionMetadata);
    }

    protected FileHandle getDirectDataFile(StatsMetadata statsMetadata, CompressionMetadata compressionMetadata)
    {
        File file = descriptor.fileFor(BtiFormat.Components.DATA);

        logger.debug("Opening direct {} ({})", descriptor, FBUtilities.prettyPrintMemory(file.length()));

        if (directFileBuilder == null)
            directFileBuilder = new FileHandle.Builder(file)
                                .withDiskAccessMode(DiskAccessMode.direct);

        return dataFileBuilder(directFileBuilder, statsMetadata, compressionMetadata).complete();
    }

    private FileHandle.Builder dataFileBuilder(FileHandle.Builder builder, StatsMetadata statsMetadata,
                                               CompressionMetadata compressionMetadata)
    {
        long recordSize = statsMetadata.estimatedPartitionSize.percentile(ioOptions.diskOptimizationEstimatePercentile);
        int bufferSize = ioOptions.diskOptimizationStrategy.bufferSize(recordSize);

        builder.bufferSize(bufferSize);
        builder.withChunkCache(chunkCache);
        builder.withDiskAccessMode(ioOptions.defaultDiskAccessMode);
        builder.withCompressionMetadata(compressionMetadata);
        builder.withCrcCheckChance(() -> tableMetadataRef.getLocal().params.crcCheckChance);
        return builder;
    }
}
