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

import io.netty.util.concurrent.FastThreadLocal;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.apache.cassandra.utils.memory.MemoryUtil;

public final class DirectThreadLocalByteBufferHolder implements ByteBufferHolder
{

    final FastThreadLocal<ByteBuffer> local = new FastThreadLocal<>()
    {
        @Override
        protected ByteBuffer initialValue()
        {
            return ByteBuffer.allocateDirect(0);
        }
    };

    private final int blockSize;

    public DirectThreadLocalByteBufferHolder(int blockSize)
    {
        this.blockSize = blockSize;
    }

    @Override
    public ByteBuffer getBuffer(int size)
    {
        int alignedSize = BitUtil.align(size, blockSize);

        ByteBuffer buffer = local.get();
        if (buffer.capacity() >= alignedSize)
        {
            buffer.clear().limit(alignedSize);
            return buffer;
        }

        MemoryUtil.clean(buffer);
        buffer = BufferUtil.allocateDirectAligned(alignedSize, blockSize);
        local.set(buffer);
        return buffer;
    }
}