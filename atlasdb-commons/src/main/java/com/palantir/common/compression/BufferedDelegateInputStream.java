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
    protected int bufferSize;

    public BufferedDelegateInputStream(InputStream delegate, int bufferSize) {
        this.delegate = delegate;
        this.position = 0;
        this.bufferSize = 0;
        allocateBuffer(bufferSize);
    }

    public BufferedDelegateInputStream(InputStream delegate) {
        this(delegate, 0);
    }

    public void allocateBuffer(int size) {
        Preconditions.checkArgument(size >= 0, "buffer size must be greater than or equal to zero");
        Preconditions.checkState(position == bufferSize, "cannot reallocate a buffer that has not been fully read");
        this.buffer = new byte[size];
    }

    @Override
    public final int read() throws IOException {
        if ((position == bufferSize) && !refill()) {
            return READ_FAILED;
        }
        return buffer[position++] & 0xff;
    }

    @Override
    public final int read(byte[] b, int off, int len) throws IOException {
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
            int bytesRead = readBytes(b, off, len, bytesReadSoFar);
            bytesReadSoFar += bytesRead;
            if (position == bufferSize) {
                boolean bufferRefilled = refill();
                if (!bufferRefilled) {
                    // We've exhausted the delegate stream and can't read any more.
                    break;
                }
            }
        }

        return bytesReadSoFar > 0 ? bytesReadSoFar : READ_FAILED;
    }

    private int readBytes(byte[] dest, int offset, int maxBytesToRead, int bytesReadSoFar) {
        int destPos = offset + bytesReadSoFar;
        int bytesLeftInBuffer = available();
        int bytesLeftToRead = maxBytesToRead - bytesReadSoFar;
        int bytesToRead = Math.min(bytesLeftToRead, bytesLeftInBuffer);
        System.arraycopy(buffer, position, dest, destPos, bytesToRead);
        position += bytesToRead;
        return bytesToRead;
    }

    @Override
    public final int available() {
        return bufferSize - position;
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
