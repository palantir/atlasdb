/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.common.compression;

import java.io.IOException;
import java.io.InputStream;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

public class LZ4CompressingInputStream extends InputStream {
    private final InputStream delegate;
    private final LZ4Compressor compressor;
    private final byte[] buffer;
    private final byte[] compressedBuffer;
    private final StreamingXXHash32 hasher;
    private final LZ4FrameDescriptor frameDescriptor;
    private int position;
    private int maxPosition;
    private boolean inFrameFooter;

    public LZ4CompressingInputStream(InputStream delegate) throws IOException {
        this(delegate, LZ4Streams.DEFAULT_FRAME_DESCRIPTOR);
    }

    public LZ4CompressingInputStream(InputStream delegate, LZ4FrameDescriptor frameDescriptor) throws IOException {
        this.delegate = delegate;
        this.compressor = LZ4Streams.compressor;
        this.buffer = LZ4Streams.newUncompressedBuffer(frameDescriptor);
        this.compressedBuffer = LZ4Streams.newCompressedBuffer(frameDescriptor);
        this.hasher = XXHashFactory.fastestInstance().newStreamingHash32(0);
        this.frameDescriptor = frameDescriptor;
        this.position = 0;
        this.maxPosition = LZ4Streams.FRAME_HEADER_LENGTH;
        this.inFrameFooter = false;
        writeFrameHeader();
    }

    @Override
    public int read() throws IOException {
        if ((position == maxPosition) && !refill()) {
            return -1;
        }
        return compressedBuffer[position++] & 0xff;
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
            System.arraycopy(compressedBuffer, position, b, off + bytesRead, bytesToRead);
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

    private void writeFrameHeader() throws IOException {
        System.arraycopy(getLittleEndianInt(LZ4Streams.MAGIC_VALUE), 0, compressedBuffer, 0, 4);
        byte[] frameDescriptorBytes = frameDescriptor.toByteArray();
        System.arraycopy(frameDescriptorBytes, 0, compressedBuffer, 4, LZ4Streams.FRAME_DESCRIPTOR_SIZE);
    }

    private byte[] getLittleEndianInt(int value) throws IOException {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) value;
        bytes[1] = (byte) (value >>> 8);
        bytes[2] = (byte) (value >>> 16);
        bytes[3] = (byte) (value >>> 24);
        return bytes;
    }

    private boolean refill() throws IOException {
        if (inFrameFooter) {
            return false;
        }

        int numRead = delegate.read(buffer, 0, frameDescriptor.maximumBlockSize);
        if (numRead == -1) {
            inFrameFooter = true;
            writeFooter();
        } else {
            if (frameDescriptor.hasContentChecksum) {
                hasher.update(buffer, 0, numRead);
            }
            compressBuffer(numRead);
        }
        return true;
    }

    private void writeFooter() throws IOException {
        System.arraycopy(getLittleEndianInt(0), 0, compressedBuffer, 0, 4);
        position = 0;
        maxPosition = 4;
        if (frameDescriptor.hasContentChecksum) {
            System.arraycopy(getLittleEndianInt(hasher.getValue()), 0, compressedBuffer, 4, 4);
            maxPosition = 8;
        }
    }

    private void compressBuffer(int bufferSize) throws IOException {
        int compressedBufferMaxSize = compressedBuffer.length - LZ4Streams.HEADER_SIZE;
        int compressedLength = compressor.compress(buffer, 0, bufferSize, compressedBuffer, LZ4Streams.HEADER_SIZE, compressedBufferMaxSize);

        if (compressedLength >= bufferSize) {
            System.arraycopy(buffer, 0, compressedBuffer, LZ4Streams.HEADER_SIZE, bufferSize);
            //For uncompressed blocks, the highest bit in the block size should be 1
            writeBlockSize(bufferSize ^ 0x80000000);
            maxPosition = LZ4Streams.HEADER_SIZE + bufferSize;
        } else {
            writeBlockSize(compressedLength);
            maxPosition = LZ4Streams.HEADER_SIZE + compressedLength;
        }
        position = 0;
    }

    private void writeBlockSize(int length) throws IOException {
        System.arraycopy(getLittleEndianInt(length), 0, compressedBuffer, 0, 4);
    }
}
