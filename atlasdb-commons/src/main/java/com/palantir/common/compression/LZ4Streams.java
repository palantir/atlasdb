/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

    static final int HEADER_SIZE = 4;

    static final int FRAME_DESCRIPTOR_SIZE = 3;

    static final int MAGIC_VALUE = 0x184D2204;
    static final int MAGIC_LENGTH = 4;

    static final int FRAME_HEADER_LENGTH = 7;

    static final LZ4FrameDescriptor DEFAULT_FRAME_DESCRIPTOR = new LZ4FrameDescriptor(false, 4);

    static byte[] newUncompressedBuffer(LZ4FrameDescriptor frameDescriptor) {
        return new byte[HEADER_SIZE + frameDescriptor.maximumBlockSize];
    }

    static byte[] newCompressedBuffer(LZ4FrameDescriptor frameDescriptor) {
        return new byte[HEADER_SIZE + compressor.maxCompressedLength(frameDescriptor.maximumBlockSize)];
    }

    static byte[] littleEndianIntToBytes(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value);
        buffer.flip();
        return buffer.array();
    }

    static int littleEndianIntFromBytes(byte[] b, int offset) {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).put(b, offset, 4);
        buffer.flip();
        return buffer.getInt();
    }
}
