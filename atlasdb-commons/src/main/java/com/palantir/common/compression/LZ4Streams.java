/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.common.compression;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

public class LZ4Streams {
    static final LZ4Factory factory = LZ4Factory.fastestInstance();
    static final LZ4Compressor compressor = factory.fastCompressor();
    static final LZ4SafeDecompressor decompressor = LZ4Factory.fastestInstance().safeDecompressor();

    static final int HEADER_SIZE = 4;

    static final int FRAME_DESCRIPTOR_SIZE = 3;

    static final int MAGIC_VALUE = 0x184D2204;
    static final int MAGIC_LENGTH = 4;

    static final int FRAME_HEADER_LENGTH = 7;

    static final LZ4FrameDescriptor DEFAULT_FRAME_DESCRIPTOR = new LZ4FrameDescriptor(false, 4);

    static final byte[] newUncompressedBuffer(LZ4FrameDescriptor frameDescriptor) {
        return new byte[HEADER_SIZE + frameDescriptor.maximumBlockSize];
    }

    static final byte[] newCompressedBuffer(LZ4FrameDescriptor frameDescriptor) {
        return new byte[HEADER_SIZE + maxCompressedSize(frameDescriptor.maximumBlockSize)];
    }

    static final int maxCompressedSize(int uncompressedSize) {
        return uncompressedSize + uncompressedSize / 255 + 16;
    }
}
