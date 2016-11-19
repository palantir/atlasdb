/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.common.compression;

import com.google.common.base.Preconditions;

import net.jpountz.xxhash.XXHashFactory;

/**
 * Specification at
 *     https://docs.google.com/document/d/1cl8N1bmkTdIpPLtnlzbBSFAdUeyNo5fwfHbHU7VRNWY
 *
 * Currently supports only three-byte frame descriptors (no content size field)
 *
 * <pre>
 * Byte 1: Flags
 *   bit 7-6: version number, must be set to 01
 *   bit 5:   block independence, we only set to 1
 *   bit 4:   block checksum, we only set to 0
 *   bit 3:   content size, we only set 0
 *   bit 2:   content checksum, can be 0 or 1
 *   bit 1-0: reserved, must be 0
 *
 * Byte 2: Block Flags
 *   bit 7:   reserved, must be 0
 *   bit 6-4: block maximum size, can be 100 to 111
 *   bit 3-0: reserved, must be 0
 *
 * Byte 3: Descriptor Checksum
 *   ((hash >> 8) & 0xFF), where hash is xxHash32 of previous bytes with 0 seed
 * </pre>
 */
public final class LZ4FrameDescriptor {
    private static final byte START_FLAG_BYTE = 0x60;
    private static final byte START_BD_FLAG_BYTE = 0x00;
    private static final byte CONTENT_CHECKSUM_FLAG = 1 << 2;
    private static final int[] BLOCK_SIZE_LOOKUP = new int[] {
            -1, -1, -1, -1, // 0-3 are N/A
            1 << 16, // 64 KB
            1 << 18, // 256 KB
            1 << 20, // 1 MB
            1 << 22  // 4 MB
    };

    private final int maximumBlockSizeIndex;
    final int maximumBlockSize;
    final boolean hasContentChecksum;

    public static LZ4FrameDescriptor fromByteArray(byte[] descriptor) {
        boolean hasContentChecksum = (descriptor[0] & CONTENT_CHECKSUM_FLAG) > 0;
        int blockSize = (descriptor[1] & 0x70) >> 4;
        return new LZ4FrameDescriptor(hasContentChecksum, blockSize);
    }

    public LZ4FrameDescriptor(boolean hasContentChecksum, int maximumBlockSizeIndex) {
        this.hasContentChecksum = hasContentChecksum;
        this.maximumBlockSizeIndex = maximumBlockSizeIndex;
        Preconditions.checkArgument(maximumBlockSizeIndex > 3 && maximumBlockSizeIndex < 8,
                "Maximum block size of %s is not valid", maximumBlockSizeIndex);
        this.maximumBlockSize = BLOCK_SIZE_LOOKUP[maximumBlockSizeIndex];
    }

    public byte[] toByteArray() {
        byte[] result = new byte[] {START_FLAG_BYTE, START_BD_FLAG_BYTE, 0x00};
        if (hasContentChecksum) {
            result[0] |= CONTENT_CHECKSUM_FLAG;
        }
        result[1] |= maximumBlockSizeIndex << 4;
        result[2] = (byte) (XXHashFactory.fastestInstance().hash32().hash(result, 0, 2, 0) >> 8 & 0xFF);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LZ4FrameDescriptor that = (LZ4FrameDescriptor) o;

        if (hasContentChecksum != that.hasContentChecksum) return false;
        return maximumBlockSizeIndex == that.maximumBlockSizeIndex;

    }

    @Override
    public int hashCode() {
        int result = (hasContentChecksum ? 1 : 0);
        result = 31 * result + maximumBlockSizeIndex;
        return result;
    }
}
