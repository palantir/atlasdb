/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.common.compression;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import com.google.common.base.Preconditions;

import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

public class LZ4DecompressingInputStream extends InputStream {
    private final InputStream delegate;
    private final LZ4SafeDecompressor decompressor;
    private final byte[] buffer;
    private final byte[] compressedBuffer;
    private final StreamingXXHash32 hasher;
    private final LZ4FrameDescriptor frameDescriptor;
    private int position;
    private int maxPosition;
    private int nextLength;

    public LZ4DecompressingInputStream(InputStream delegate) throws IOException {
        this.delegate = delegate;
        this.decompressor = LZ4Streams.decompressor;
        this.hasher = XXHashFactory.fastestInstance().newStreamingHash32(0);
        byte[] actualMagic =
                new byte[LZ4Streams.HEADER_SIZE + LZ4Streams.MAGIC_LENGTH + LZ4Streams.FRAME_DESCRIPTOR_SIZE];
        this.nextLength = readBlock(actualMagic, actualMagic.length);
        Preconditions.checkState(readIntLittleEndian(actualMagic, 0) == LZ4Streams.MAGIC_VALUE,
                "Input is not an lz4 stream that this input stream can decompress");
        this.frameDescriptor = LZ4FrameDescriptor.fromByteArray(Arrays.copyOfRange(actualMagic,
                LZ4Streams.MAGIC_LENGTH, LZ4Streams.MAGIC_LENGTH + LZ4Streams.FRAME_DESCRIPTOR_SIZE));
        this.buffer = LZ4Streams.newUncompressedBuffer(frameDescriptor);
        this.compressedBuffer = LZ4Streams.newCompressedBuffer(frameDescriptor);
        this.position = 0;
        this.maxPosition = 0;
    }

    @Override
    public int read() throws IOException {
        if ((position == maxPosition) && !refill()) {
            return -1;
        }
        return buffer[position++] & 0xff;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int bytesRead = 0;
        while (bytesRead < len) {
            int remainingBuffer = maxPosition - position;
            int bytesToRead = Math.min(len - bytesRead, remainingBuffer);
            System.arraycopy(buffer, position, b, off + bytesRead, bytesToRead);
            position += bytesToRead;
            bytesRead += bytesToRead;
            if ((position == maxPosition) && !refill()) {
                break;
            }
        }
        return bytesRead > 0 ? bytesRead : -1;
    }

    @Override
    public int available() throws IOException {
        return maxPosition - position;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    // This reads one block plus the size of the next block in as few
    // read calls as possible. The length should be the number of bytes
    // of data plus HEADER_SIZE bytes for the length of the next block.
    private int readBlock(byte[] b, int length) throws IOException {
        int read = 0;
        while (read < length) {
            int r = delegate.read(b, read, length - read);
            if (r < 0) {
                throw new EOFException("Stream ended prematurely");
            }
            read += r;
        }
        return readIntLittleEndian(b, length - 4);
    }

    protected static int readIntLittleEndian(byte[] b, int offset) throws IOException {
        return (b[offset] & 0xff) |
                ((b[offset + 1] & 0xff) << 8) |
                ((b[offset + 2] & 0xff) << 16) |
                ((b[offset + 3] & 0xff) << 24);
    }

    private boolean refill() throws IOException {
        int length = nextLength;

        //End of stream
        if (length == 0) {
            if (frameDescriptor.hasContentChecksum) {
                verifyContentChecksum();
            }
            return false;
        }

        if (length > 0) {
            //Compressed block
            nextLength = readBlock(compressedBuffer, length + LZ4Streams.HEADER_SIZE);
            maxPosition = decompressor.decompress(compressedBuffer, 0, length, buffer, 0, frameDescriptor.maximumBlockSize);
        } else {
            //Uncompressed block
            length ^= 0x80000000;
            nextLength = readBlock(buffer, length + LZ4Streams.HEADER_SIZE);
            maxPosition = length;
        }

        if (frameDescriptor.hasContentChecksum) {
            hasher.update(buffer, 0, maxPosition);
        }

        position = 0;
        return true;
    }

    private void verifyContentChecksum() throws IOException {
        byte[] bytes = new byte[4];
        if (delegate.read(bytes) < 4) {
            throw new EOFException("Could not verify stream checksum because stream ended prematurely");
        }
        int computedChecksum = hasher.getValue();
        int providedChecksum = readIntLittleEndian(bytes, 0);
        Preconditions.checkState(computedChecksum == providedChecksum,
                "LZ4 content did not match checksum: content sums to %s, but checksum was %s",
                computedChecksum, providedChecksum);
    }
}
