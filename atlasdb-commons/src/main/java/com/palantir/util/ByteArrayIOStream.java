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
package com.palantir.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Simple io stream designed for passing bytes from an output
 * stream to an input stream (or multiple input streams).
 * <p>
 * The pattern
 * <pre>
 * ByteArrayOutputStream baos = new ByteArrayOutputStream();
 * baos.write(...);
 * ByteArrayInputStream bias = new ByteArrayInputStream(baos.toByteArray());
 * </pre>has to create a copy of the underlying byte array for the
 * input stream. In contrast,
 * <pre>
 * ByteArrayIOStream byteStream = new ByteArrayIOStream();
 * byteStream.write(...);
 * InputStream in = byteStream.getInputStream();
 * </pre>avoids this extra copy.
 * <p>
 * Output stream and input stream characteristics are the same as
 * {@link ByteArrayOutputStream} and {@link ByteArrayInputStream},
 * respectively, except that all methods are unsynchronized.
 */
public class ByteArrayIOStream extends OutputStream {
    private byte[] bytes;
    private int outputPos = 0;

    public ByteArrayIOStream() {
        bytes = new byte[1024];
    }

    public ByteArrayIOStream(int initialCapacity) {
        bytes = new byte[initialCapacity];
    }

    /**
     * Returns the number of bytes written to this stream.
     */
    public int size() {
        return outputPos;
    }

    /**
     * Returns a copy of the bytes written to this stream.
     */
    public byte[] toByteArray() {
        byte[] copy = new byte[outputPos];
        System.arraycopy(bytes, 0, copy, 0, outputPos);
        return copy;
    }

    /**
     * Returns an input stream that reads from the bytes in this stream.
     * Only bytes written to the stream before calling this method are
     * available to the input stream.
     */
    public InputStream getInputStream() {
        return new Input(size());
    }

    private void ensureCapacity(int index) {
        if (bytes.length <= index) {
            int minLength = index + 1;
            int newLength = bytes.length << 1;
            if (newLength - minLength < 0) {
                newLength = minLength;
            }
            if (newLength < 0) {
                if (minLength < 0) {
                    throw new OutOfMemoryError();
                }
                newLength = Integer.MAX_VALUE;
            }
            byte[] newBytes = new byte[newLength];
            System.arraycopy(bytes, 0, newBytes, 0, outputPos);
            bytes = newBytes;
        }
    }

    @Override
    public void write(int b) throws IOException {
        ensureCapacity(outputPos);
        bytes[outputPos++] = (byte) b;
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        ensureCapacity(outputPos + len - 1);
        System.arraycopy(b, off, bytes, outputPos, len);
        outputPos += len;
    }

    private final class Input extends InputStream {
        private int pos = 0;
        private int mark = 0;
        private final int size;

        private Input(int size) {
            this.size = size;
        }

        @Override
        public int read() throws IOException {
            if (pos >= size) {
                return -1;
            }
            return bytes[pos++] & 0xff;
        }

        @Override
        public int read(byte b[], int off, int len) {
            if (b == null) {
                throw new NullPointerException();
            } else if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return 0;
            }

            int bytesRemaining = size - pos;
            if (bytesRemaining <= 0) {
                return -1;
            } else if (bytesRemaining < len) {
                System.arraycopy(bytes, pos, b, off, bytesRemaining);
                pos = size;
                return bytesRemaining;
            } else {
                System.arraycopy(bytes, pos, b, off, len);
                pos += len;
                return len;
            }
        }

        @Override
        public long skip(long n) {
            int bytesRemaining = size - pos;
            if (bytesRemaining < n) {
                pos = size;
                return bytesRemaining;
            } else {
                pos += n;
                return n;
            }
        }

        @Override
        public int available() {
            return size - pos;
        }

        @Override
        public boolean markSupported() {
            return true;
        }

        @Override
        public void mark(int _readAheadLimit) {
            mark = pos;
        }

        @Override
        public void reset() {
            pos = mark;
        }
    }
}
