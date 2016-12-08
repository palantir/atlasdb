/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.common.compression;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

public final class LZ4Streams {
    static final LZ4Compressor compressor;
    static final LZ4SafeDecompressor decompressor;
    static {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        compressor = factory.fastCompressor();
        decompressor = factory.safeDecompressor();
    }

    static final int FRAME_DESCRIPTOR_LENGTH = 3;

    static final int MAGIC_VALUE = 0x184D2204;
    static final int MAGIC_LENGTH = 4;

    static final int FRAME_HEADER_LENGTH = MAGIC_LENGTH + FRAME_DESCRIPTOR_LENGTH;

    static final int BLOCK_HEADER_LENGTH = 4;

    static final LZ4FrameDescriptor DEFAULT_FRAME_DESCRIPTOR = new LZ4FrameDescriptor(false, 4);

    static int getUncompressedBufferSize(LZ4FrameDescriptor frameDescriptor) {
        return BLOCK_HEADER_LENGTH + frameDescriptor.getMaximumBlockSize();
    }

    static int getCompressedBufferSize(LZ4FrameDescriptor frameDescriptor) {
        return BLOCK_HEADER_LENGTH + compressor.maxCompressedLength(frameDescriptor.getMaximumBlockSize());
    }

    static byte[] intToLittleEndianBytes(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value);
        buffer.flip();
        return buffer.array();
    }

    static int intFromLittleEndianBytes(byte[] b, int offset) {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).put(b, offset, 4);
        buffer.flip();
        return buffer.getInt();
    }
}
