/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.common.compression;

import com.palantir.logsafe.Preconditions;
import java.io.IOException;
import java.io.InputStream;

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
    protected final byte[] buffer;

    // Current position in the buffer
    private int position;
    // Size in bytes of the data in the buffer
    private int bufferSize;

    public BufferedDelegateInputStream(InputStream delegate, int bufferLength) {
        Preconditions.checkArgument(bufferLength >= 0, "buffer size must be greater than or equal to zero");
        this.delegate = delegate;
        this.position = 0;
        this.bufferSize = 0;
        this.buffer = new byte[bufferLength];
    }

    @Override
    public final int read() throws IOException {
        if (!ensureBytesAvailable()) {
            return READ_FAILED;
        }
        return buffer[position++] & 0xFF;
    }

    // If all bytes in the buffer have been read, attempt to refill
    // the buffer. If the buffer can't be refilled, then we've reached
    // the end of the stream and return false.
    private boolean ensureBytesAvailable() throws IOException {
        if (position == bufferSize) {
            position = BUFFER_START;
            bufferSize = refill();
            if (bufferSize == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads up to {@code len} bytes into the provided byte array {@code b}.
     * If no remaining unread bytes are available in the internal buffer,
     * {@link #refill() refill} is called to refill the internal buffer.
     * Otherwise, either {@code len} bytes or the number of bytes available
     * in the buffer are read, whichever is smaller.
     *
     * To read from the buffer until either the end of the stream is reached
     * or {@code len} bytes have been read, use
     * {@link com.google.common.io.ByteStreams#read(InputStream, byte[], int, int)
     * ByteStreams.read}.
     */
    @Override
    public final int read(byte[] b, int off, int len) throws IOException {
        Preconditions.checkNotNull(b, "Provided byte array b cannot be null.");
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        if (!ensureBytesAvailable()) {
            return READ_FAILED;
        }

        return readBytes(b, off, len);
    }

    private int readBytes(byte[] dest, int offset, int maxBytesToRead) {
        int bytesLeftInBuffer = available();
        int bytesToRead = Math.min(maxBytesToRead, bytesLeftInBuffer);
        System.arraycopy(buffer, position, dest, offset, bytesToRead);
        position += bytesToRead;
        return bytesToRead;
    }

    @Override
    public final int available() {
        return bufferSize - position;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /**
     * Refills the internal buffer from the delegate {@link InputStream}.
     *
     * @return The number of bytes written to the buffer while refilling it.
     */
    protected abstract int refill() throws IOException;

}
