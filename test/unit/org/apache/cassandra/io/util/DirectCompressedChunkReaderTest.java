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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
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

    private static Gen<Integer> mixedChunkLengths()
    {
        int minLength = 1024;
        int maxLength = 1024 * 64;
        return Gens.pick(Stream.iterate(minLength, n -> n <= maxLength, n -> n * 2)
                               .collect(Collectors.toList()));
    }

    @Test
    public void compressedReads()
    {
        var optionGen = writerOptions();
        var paramsGen = compressionParams(mixedChunkLengths());
        var fileLengthGen = Gens.longs().between(1, 1 << 16);

        testReads(optionGen, paramsGen, fileLengthGen, false);
    }

    @Test
    public void scanCompressedReads()
    {
        var optionGen = writerOptions();
        var paramsGen = compressionParams(mixedChunkLengths());
        var fileLengthGen = Gens.longs().between(1, 1 << 16);

        testReads(optionGen, paramsGen, fileLengthGen, true);
    }

    @Test
    public void compressedReads_fileLengthLessThanChunkLength()
    {
        var optionGen = writerOptions();
        var chunkLengths = Gens.constant(4096);
        var paramsGen = compressionParams(chunkLengths);
        var fileLengthGen = Gens.longs().of(1024);

        testReads(optionGen, paramsGen, fileLengthGen, true);
    }

    @Test
    public void compressedReads_partialTrailingChunk()
    {
        var optionGen = writerOptions();
        int chunkLength = 4096;
        var chunkLengths = Gens.constant(chunkLength);
        var paramsGen = compressionParams(chunkLengths);
        var fileLengthGen = Gens.longs().of(chunkLength + 96);

        testReads(optionGen, paramsGen, fileLengthGen, true);
    }

    @Test
    public void corruptBlock()
    {
        Gen<Integer> chunkLength = Gens.constant(4096);
        Gen<CompressionParams> compressionGen = compressionParams(chunkLength);

        qt().withSeed(seed).forAll(Gens.random(), compressionGen).check((rs, params) -> {
            long totalBytesToWrite = params.chunkLength() * 2L; // two chunks

            List<ByteBuffer> chunks = generateRandomChunks(rs, params, totalBytesToWrite);

            File dataFile = new File(JAVA_IO_TMPDIR.getString() + "data_corrupt_block.bin");

            final CompressionMetadata metadata;

            try (CompressedSequentialWriter writer = getCompressedSequentialWriter(writerOption(1 << 10), params, dataFile))
            {
                for (ByteBuffer chunk : chunks)
                    writer.write(chunk.duplicate());
                writer.sync();
                metadata = writer.open(0);
            }

            CompressionMetadata.Chunk firstChunkMeta = metadata.chunkFor(0);

            long truncatePoint = firstChunkMeta.offset + (long) firstChunkMeta.length / 2; // Halfway into the chunk

            try (FileChannel fileChannel = FileChannel.open(dataFile.toPath(), StandardOpenOption.WRITE))
            {
                fileChannel.truncate(truncatePoint);
            }

            boolean exceptionThrown = false;
            try
            {
                readAndVerifyChunks(dataFile, metadata, chunks, totalBytesToWrite, false);
            }
            catch (CorruptSSTableException exc)
            {
                exceptionThrown = true;
                Assert.assertTrue("Exception message should indicate corrupt SSTable", exc.getMessage().contains(dataFile.name()));
            }
            finally
            {
                metadata.close();
                Files.deleteIfExists(dataFile.toPath());
            }
            Assert.assertTrue("Expected CorruptSSTableException for truncated chunk, but none was thrown.", exceptionThrown);
        });
    }

    @Test
    public void uncompressedReads()
    {
        int maxCompressedLength = 0; // force uncompressed path (chunk.length > maxCompressedLength)
        CompressionParams uncompressed = CompressionParams.lz4(DEFAULT_CHUNK_LENGTH, maxCompressedLength);

        var optionGen = writerOptions();
        var paramsGen = Gens.constant(uncompressed);
        var fileLengthGen = Gens.longs().between(1, 1 << 16);

        testReads(optionGen, paramsGen, fileLengthGen, false);
    }

    private static void testReads(Gen<SequentialWriterOption> optionGen, Gen<CompressionParams> paramsGen, Gen.LongGen totalBytesGen, boolean forScan)
    {
        qt().withSeed(seed).forAll(Gens.random(), optionGen, paramsGen).check((rs, option, params) -> {
            long totalBytesToWrite = totalBytesGen.nextLong(rs);
            List<ByteBuffer> chunks = generateRandomChunks(rs, params, totalBytesToWrite);

            File file = new File(JAVA_IO_TMPDIR.getString() + "data.bin");
            try (CompressedSequentialWriter writer = getCompressedSequentialWriter(option, params, file))
            {
                for (ByteBuffer chunk : chunks)
                    writer.write(chunk.duplicate());

                writer.sync();

                CompressionMetadata metadata = writer.open(0);
                readAndVerifyChunks(file, metadata, chunks, totalBytesToWrite, forScan);
            }
            finally
            {
                Files.deleteIfExists(file.toPath());
            }
        });
    }

    private static void readAndVerifyChunks(File file, CompressionMetadata metadata, List<ByteBuffer> expectedChunksData, long totalBytesExpected,
                                            boolean forScan)
    {
        ByteBuffer readBuffer = ByteBuffer.allocateDirect(metadata.chunkLength());

        try (ChannelProxy channel = new ChannelProxy(file, ChannelProxy.IOMode.DIRECT);
             CompressedChunkReader reader = new CompressedChunkReader.Direct(channel, metadata, () -> 1d);
             metadata)
        {
            if (forScan)
                reader.forScan();

            long currentFileOffset = 0;
            long totalBytesRead = 0;
            int currentChunkIndex = 0;

            while (totalBytesRead < totalBytesExpected)
            {
                ByteBuffer currentExpectedChunk = expectedChunksData.get(currentChunkIndex);

                readBuffer.clear();
                reader.readChunk(currentFileOffset, readBuffer);

                Assert.assertTrue("Read buffer is empty unexpectedly at offset " + currentFileOffset, readBuffer.hasRemaining());

                int actualBytesRead = readBuffer.remaining();
                int expectedBytes = currentExpectedChunk.remaining();

                Assert.assertTrue("Read buffer remaining (" + actualBytesRead + ") is less than expected (" + expectedBytes + ") at offset " + currentFileOffset,
                                  actualBytesRead >= expectedBytes);

                int originalReadBufferLimit = readBuffer.limit();
                readBuffer.limit(expectedBytes);
                Assert.assertEquals("Mismatched data at offset " + currentFileOffset, currentExpectedChunk, readBuffer);
                readBuffer.limit(originalReadBufferLimit);

                totalBytesRead += expectedBytes;
                currentFileOffset += metadata.chunkLength();
                currentChunkIndex++;
            }
        }
        finally
        {
            FileUtils.clean(readBuffer);
        }
    }

    private static List<ByteBuffer> generateRandomChunks(RandomSource rs, CompressionParams params, long bytesToWrite)
    {
        List<ByteBuffer> chunks = new ArrayList<>();
        long bytesGenerated = 0;
        while (bytesGenerated < bytesToWrite)
        {
            ByteBuffer chunkBuffer = ByteBuffer.allocate(params.chunkLength());
            int bytesToFill = (int) Math.min(chunkBuffer.capacity(), bytesToWrite - bytesGenerated);
            byte[] tempBytes = new byte[bytesToFill];
            rs.nextBytes(tempBytes);
            chunkBuffer.put(tempBytes);
            chunkBuffer.flip();
            chunks.add(chunkBuffer);
            bytesGenerated += bytesToFill;
        }
        return chunks;
    }

    private static CompressedSequentialWriter getCompressedSequentialWriter(SequentialWriterOption option, CompressionParams params, File dataFile)
    {
        return new CompressedSequentialWriter(dataFile, new File("file.offset"), new File("file.digest"), option, params, new MetadataCollector(new ClusteringComparator()));
    }
}