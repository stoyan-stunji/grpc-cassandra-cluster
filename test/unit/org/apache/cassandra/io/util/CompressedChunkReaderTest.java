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
import java.util.stream.Stream;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.schema.CompressionParams;
import org.assertj.core.api.Assertions;

public abstract class CompressedChunkReaderTest
{

    static Gen<SequentialWriterOption> writerOptions()
    {
        Gen<Integer> bufferSizes = Gens.constant(1 << 10); //.pickInt(1 << 4, 1 << 10, 1 << 15);
        return rs -> SequentialWriterOption.newBuilder()
                                           .finishOnClose(false)
                                           .bufferSize(bufferSizes.next(rs))
                                           .build();
    }

    enum CompressionKind
    {
        Noop, Snappy, Deflate, Lz4, Zstd
    }

    static Gen<Integer> mixedChunkLengths()
    {
        int minLength = 1024;
        int maxLength = 1024 * 64;
        return Gens.pick(Stream.iterate(minLength, n -> n <= maxLength, n -> n * 2)
                               .toList());
    }

    static Gen<CompressionParams> compressionParams(Gen<Integer> chunkLengths)
    {
        Gen<Double> compressionRatio = Gens.pick(1.1D);
        return rs -> {
            CompressionKind kind = rs.pick(CompressionKind.values());
            switch (kind)
            {
                case Noop:
                    return CompressionParams.noop();
                case Snappy:
                    return CompressionParams.snappy(chunkLengths.next(rs), compressionRatio.next(rs));
                case Deflate:
                    return CompressionParams.deflate(chunkLengths.next(rs));
                case Lz4:
                    return CompressionParams.lz4(chunkLengths.next(rs));
                case Zstd:
                    return CompressionParams.zstd(chunkLengths.next(rs));
                default:
                    throw new UnsupportedOperationException(kind.name());
            }
        };
    }

    protected void doReads(File f, CompressionMetadata metadata, long length, boolean useReadAhead)
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(metadata.chunkLength());

        try (ChannelProxy channel = getChannel(f);
             CompressedChunkReader reader = getReader(channel, metadata);
             metadata)
        {
            if (useReadAhead)
                reader.forScan();

            long offset = 0;
            long maxOffset = length * Long.BYTES;
            do
            {
                reader.readChunk(offset, buffer);
                for (long expected = offset / Long.BYTES; buffer.hasRemaining(); expected++)
                    Assertions.assertThat(buffer.getLong()).isEqualTo(expected);

                offset += metadata.chunkLength();
            }
            while (offset < maxOffset);
        }
        finally
        {
            FileUtils.clean(buffer);
        }
    }

    protected ChannelProxy getChannel(File file)
    {
        return new ChannelProxy(file);
    }

    protected abstract CompressedChunkReader getReader(ChannelProxy channel, CompressionMetadata metadata);
}
