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

package org.apache.cassandra.io.util;

import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompressionParams;

import static accord.utils.Property.qt;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_IO_TMPDIR;
import static org.apache.cassandra.schema.CompressionParams.DEFAULT_CHUNK_LENGTH;

public class DirectCompressedChunkReaderTest extends CompressedChunkReaderTest
{

    private static final Logger logger = LoggerFactory.getLogger(DirectCompressedChunkReaderTest.class);
    private static int seed;

    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    @BeforeClass
    public static void setup()
    {
        seed = new Random().nextInt();
        logger.info("Seed: {}", seed);
    }

    @Test
    public void testCompressedReads()
    {
        var optionGen = writerOptions();
        var paramsGen = compressionParams(mixedChunkLengths());
        var lengthGen = Gens.longs().between(1, 1 << 16);

        testReads(optionGen, paramsGen, lengthGen, true);
    }

    @Test
    public void testCompressedReads_fileLengthLessThanChunkLength()
    {
        var optionGen = writerOptions();
        var chunkLengths = Gens.constant(4096);
        var paramsGen = compressionParams(chunkLengths);
        var lengthGen = Gens.longs().of(1024);

        testReads(optionGen, paramsGen, lengthGen, true);
    }

    @Test
    public void testCompressedReads_partialTrailingChunk()
    {
        var optionGen = writerOptions();
        int chunkLength = 4096;
        var chunkLengths = Gens.constant(chunkLength);
        var paramsGen = compressionParams(chunkLengths);
        var lengthGen = Gens.longs().of(chunkLength + 96);

        testReads(optionGen, paramsGen, lengthGen, true);
    }

    @Test
    public void testUncompressedReads()
    {
        int maxCompressedLength = 0; // force uncompressed path (chunk.length > maxCompressedLength)
        CompressionParams uncompressed = CompressionParams.lz4(DEFAULT_CHUNK_LENGTH, maxCompressedLength);

        var optionGen = writerOptions();
        var paramsGen = Gens.constant(uncompressed);
        var lengthGen = Gens.longs().between(1, 1 << 16);

        testReads(optionGen, paramsGen, lengthGen, false);
    }

    private void testReads(Gen<SequentialWriterOption> optionGen, Gen<CompressionParams> paramsGen, Gen.LongGen lengthGen,
                           boolean withScan)
    {
        qt().withSeed(seed).forAll(Gens.random(), optionGen, paramsGen).check((rs, option, params) -> {
            // Skipping JIMFS for Direct I/O tests due to its lack of physical disk alignment simulation.
            String fileName = JAVA_IO_TMPDIR.getString() + "data.bin";

            File f = new File(fileName);
            long length = lengthGen.nextLong(rs);
            CompressionMetadata metadata1, metadata2;
            try (CompressedSequentialWriter writer = new CompressedSequentialWriter(f, new File("/file.offset"), new File("/file.digest"), option, params, new MetadataCollector(new ClusteringComparator())))
            {
                for (long i = 0; i < length; i++)
                    writer.writeLong(i);

                writer.sync();
                metadata1 = writer.open(0);
                metadata2 = withScan ? writer.open(0) : null;
            }

            doReads(f, metadata1, length, false);

            if (withScan)
            {
                doReads(f, metadata2, length, true);
            }
        });
    }

    @Override
    protected ChannelProxy getChannel(File file)
    {
        return new ChannelProxy(file, ChannelProxy.IOMode.DIRECT);
    }

    @Override
    protected CompressedChunkReader getReader(ChannelProxy channel, CompressionMetadata metadata)
    {
        return new CompressedChunkReader.Direct(channel, metadata, () -> 1d);
    }
}