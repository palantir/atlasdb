/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.common.compression;

import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Preconditions;

/**
 * {@link InputStream} that wraps a delegate InputStream, buffering reads
 * to the delegate. Allows a customized strategy for refilling the internal
 * buffer from the delegate stream for use cases such as decompressing a
 * compressed stream.
 */
public abstract class BufferedDelegateInputStream extends InputStream {

    protected static final int READ_FAILED = -1;
    protected static final int BUFFER_START = 0;

    protected final InputStream delegate;

    // Internal buffer from which data is read
    protected byte[] buffer;
    // Current position in the buffer
    protected int position;
    // Size in bytes of the data in the buffer
    protected int maxPosition;

    public BufferedDelegateInputStream(InputStream delegate, int bufferSize) {
        this.delegate = delegate;
        this.position = 0;
        this.maxPosition = 0;
        allocateBuffer(bufferSize);
    }

    public BufferedDelegateInputStream(InputStream delegate) {
        this(delegate, 0);
    }

    public void allocateBuffer(int bufferSize) {
        Preconditions.checkArgument(bufferSize >= 0, "buffer size must be greater than or equal to zero");
        Preconditions.checkArgument(position == maxPosition, "cannot reallocate a buffer that has not been fully read");
        this.buffer = new byte[bufferSize];
    }

    @Override
    public final int read() throws IOException {
        if ((position == maxPosition) && !refill()) {
            return READ_FAILED;
        }
        return buffer[position++] & 0xff;
    }

    @Override
    public final int read(byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        // Read from the buffer until the delegate stream is exhausted
        // or len bytes have been read. Whenever the internal buffer is
        // exhausted, refill is called to refresh the buffer from the
        // delegate input stream.
        int bytesReadSoFar = 0;
        while (bytesReadSoFar < len) {
            int remainingBuffer = maxPosition - position;
            int bytesToRead = Math.min(len - bytesReadSoFar, remainingBuffer);
            System.arraycopy(buffer, position, b, off + bytesReadSoFar, bytesToRead);
            position += bytesToRead;
            bytesReadSoFar += bytesToRead;
            if ((position == maxPosition) && !refill()) {
                break;
            }
        }

        return bytesReadSoFar > 0 ? bytesReadSoFar : READ_FAILED;
    }

    @Override
    public final int available() throws IOException {
        return maxPosition - position;
    }

    @Override
    public final void close() throws IOException {
        delegate.close();
    }

    /**
     * Refills the internal buffer from the delegate {@link InputStream}.
     *
     * @return True if the buffer was refilled. False if the delegate
     *         stream has been exhausted and the internal buffer is empty.
     */
    protected abstract boolean refill() throws IOException;

}
