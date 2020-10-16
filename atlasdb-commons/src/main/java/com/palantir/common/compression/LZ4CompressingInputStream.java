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

import com.google.common.io.ByteStreams;
import com.palantir.logsafe.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Checksum;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

/**
 * {@link InputStream} that wraps a delegate InputStream, compressing its
 * contents as they are read. Compression is managed via a compressing
 * {@link OutputStream}. Reads pull data as necessary from the delegate
 * InputStream, write the uncompressed data through the OutputStream, then
 * serve the read from the resulting compressed buffer.
 */
public final class LZ4CompressingInputStream extends BufferedDelegateInputStream {

    private static final LZ4Compressor COMPRESSOR = LZ4Factory.fastestInstance().fastCompressor();
    private static final int DEFAULT_SEED = 0x9747b28c;
    private static final int DEFAULT_BLOCK_SIZE = 1 << 16; // 64 KB
    private static final int LZ4_HEADER_SIZE = 21;

    private final LZ4BlockOutputStream compressingStream;
    private final int blockSize;
    private final byte[] uncompressedBuffer;

    // Position in the compressed buffer while writing
    private int writeBufferPosition;
    // Flag to indicate whether this stream has been exhausted.
    private boolean finished;

    public LZ4CompressingInputStream(InputStream delegate) {
        this(delegate, DEFAULT_BLOCK_SIZE);
    }

    public LZ4CompressingInputStream(InputStream delegate, int blockSize) {
        super(delegate, LZ4_HEADER_SIZE + COMPRESSOR.maxCompressedLength(blockSize));
        this.blockSize = blockSize;
        this.uncompressedBuffer = new byte[blockSize];
        Checksum checksum =
                XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum();
        OutputStream delegateOutputStream = new InternalByteArrayOutputStream();
        this.compressingStream = new LZ4BlockOutputStream(delegateOutputStream, blockSize, COMPRESSOR, checksum, true);
        this.finished = false;
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
            // Lastly, we flush the LZ4 footer into the internal buffer.
            compressingStream.finish();
            compressingStream.flush();
            finished = true;
        } else {
            // Write the bytes to the compressing stream and flush it.
            compressingStream.write(uncompressedBuffer, BUFFER_START, bytesRead);
            compressingStream.flush();
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
        Preconditions.checkState(buffer.length >= size, "Internal buffer overflow");
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        compressingStream.close();
    }

    /**
     * Internal {@link OutputStream} that wraps the pre-existing buffer. This
     * is an inner class since LZ4CompressingInputStream cannot extend both
     * {@link BufferedDelegateInputStream} and {@link OutputStream}.
     */
    private final class InternalByteArrayOutputStream extends OutputStream {

        public InternalByteArrayOutputStream() {
            super();
        }

        @Override
        public void write(int b) throws IOException {
            LZ4CompressingInputStream.this.write(b);
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException {
            LZ4CompressingInputStream.this.write(b, off, len);
        }
    }
}
