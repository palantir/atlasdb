/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.io.ByteStreams;
import com.palantir.logsafe.Preconditions;

/**
 * {@link InputStream} that wraps a delegate InputStream, compressing its
 * contents as they are read. Compression is managed via a compressing
 * {@link OutputStream}. Reads pull data as necessary from the delegate
 * InputStream, write the uncompressed data through the OutputStream, then
 * serve the read from the resulting compressed buffer.
 */
public abstract class AbstractCompressingInputStream extends BufferedDelegateInputStream {

    private final int blockSize;
    private final byte[] uncompressedBuffer;

    // Position in the compressed buffer while writing
    private int writeBufferPosition;
    // Flag to indicate whether this stream has been exhausted.
    private boolean finished;


    public AbstractCompressingInputStream(InputStream delegate, int blockSize, int internalBufferSize) throws IOException {
        this(delegate, blockSize, internalBufferSize, 0);
    }

    public AbstractCompressingInputStream(InputStream delegate, int blockSize, int internalBufferSize, int initalBufferPosition) throws IOException {
        super(delegate, internalBufferSize, initalBufferPosition);
        this.blockSize = blockSize;
        this.uncompressedBuffer = new byte[blockSize];
        this.finished = false;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        getCompressionOutputStream().close();
    }


    @Override
    protected int refill() throws IOException {
        if (finished) {
            return 0;
        }
        writeBufferPosition = 0;

        // Read a block of data from the delegate input stream.
        int bytesRead = ByteStreams.read(delegate, uncompressedBuffer, BUFFER_START, blockSize);
        if (bytesRead == 0) {
            // The delegate input stream has been exhausted, so we notify
            // the compressing stream that there are no remaining bytes.
            finishOutputStream();
            getCompressionOutputStream().flush();

            finished = true;
        } else {
            // Write the bytes to the compressing stream and flush it.
            getCompressionOutputStream().write(uncompressedBuffer, BUFFER_START, bytesRead);
            getCompressionOutputStream().flush();
        }

        return writeBufferPosition;
    }

    private void write(int b) throws IOException {
        ensureCapacity(writeBufferPosition + 1);
        buffer[writeBufferPosition] = (byte) b;
        writeBufferPosition++;
    }

    private void write(byte b[], int off, int len) throws IOException {
        Preconditions.checkNotNull(b, "Provided byte array b cannot be null.");
        if ((off < 0) || (len < 0) || (off + len > b.length)) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }
        ensureCapacity(writeBufferPosition + len);
        System.arraycopy(b, off, buffer, writeBufferPosition, len);
        writeBufferPosition += len;
    }

    // Since the internal buffer size doesn't change, we throw if
    // someone tries to write past the end of the buffer.
    private void ensureCapacity(int size) {
        String message = "Internal buffer overflow. Buffer " + (size - buffer.length) + "bytes shorter";
        Preconditions.checkState(buffer.length >= size, message );
    }



    /**
     * Internal {@link OutputStream} that wraps the pre-existing buffer. This
     * is an inner class since LZ4CompressingInputStream cannot extend both
     * {@link BufferedDelegateInputStream} and {@link OutputStream}.
     */
    protected final class InternalByteArrayOutputStream extends OutputStream {

        public InternalByteArrayOutputStream() {
            super();
        }

        @Override
        public void write(int b) throws IOException {
            AbstractCompressingInputStream.this.write(b);
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException {
            AbstractCompressingInputStream.this.write(b, off, len);
        }
    }

    protected abstract OutputStream getCompressionOutputStream();

    protected abstract void finishOutputStream() throws IOException;

}
