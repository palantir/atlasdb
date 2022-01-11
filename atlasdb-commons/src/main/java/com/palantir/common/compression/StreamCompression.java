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

import com.google.common.io.Closeables;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.exceptions.SafeIoException;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import net.jpountz.lz4.LZ4BlockInputStream;

public enum StreamCompression {
    GZIP,
    LZ4,
    NONE;

    private static final byte[] gzipMagic = GzipCompressingInputStream.getMagicPrefix();
    private static final byte[] lz4Magic = "LZ4Block".getBytes(StandardCharsets.UTF_8);
    static final int DEFAULT_BLOCK_SIZE = 1 << 16; // 64 KiB

    public InputStream compress(InputStream stream) {
        switch (this) {
            case GZIP:
                return GzipCompressingInputStream.compress(stream, DEFAULT_BLOCK_SIZE);
            case LZ4:
                return new LZ4CompressingInputStream(stream);
            case NONE:
                return stream;
        }
        throw new SafeIllegalStateException("Unreachable code");
    }

    public InputStream decompress(InputStream stream) {
        switch (this) {
            case NONE:
                return stream;
            case GZIP:
            case LZ4:
                return decompressWithHeader(stream);
        }
        throw new SafeIllegalStateException("Unreachable code");
    }

    private static boolean startsWith(InputStream stream, byte[] data) throws IOException {
        stream.mark(data.length);
        try {
            for (byte datum : data) {
                if (stream.read() != Byte.toUnsignedInt(datum)) {
                    return false;
                }
            }
            return true;
        } finally {
            stream.reset();
        }
    }

    private static InputStream decompressWithHeader(InputStream unbuffered) {
        try {
            BufferedInputStream stream = new BufferedInputStream(unbuffered, DEFAULT_BLOCK_SIZE);
            if (startsWith(stream, gzipMagic)) {
                return new GZIPInputStream(stream);
            } else if (startsWith(stream, lz4Magic)) {
                return new LZ4BlockInputStream(stream);
            } else {
                return new ThrowingInputStream(new UnsupportedOperationException("Unknown compression scheme"));
            }
        } catch (IOException e) {
            Closeables.closeQuietly(unbuffered);
            // This avoids awkward cases of us having to close many returned InputStreams in wrapping code.
            return new ThrowingInputStream(e);
        }
    }

    @SuppressWarnings("InputStreamSlowMultibyteRead") // Always throws
    private static final class ThrowingInputStream extends InputStream {
        private final Throwable thrown;

        private ThrowingInputStream(Throwable thrown) {
            this.thrown = thrown;
        }

        @Override
        public int read() throws IOException {
            throw new SafeIoException("Could not construct decompressed stream", thrown);
        }
    }
}
