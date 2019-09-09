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
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

public final class GzipCompressingInputStream extends AbstractCompressingInputStream {

    private final GZIPOutputStream compressingStream;
    private static final int DEFAULT_BLOCK_SIZE = 1 << 16; // 524 KB
    private static final int GZIP_HEADER_SIZE = 10;
    private static final int GZIP_TRAILER_SIZE = 8;


    public GzipCompressingInputStream(InputStream delegate, int blockSize) throws IOException {
        super(delegate, blockSize, maxCompressedLength(blockSize), GZIP_HEADER_SIZE);
        OutputStream delegateOutputStream = new InternalByteArrayOutputStream();
        this.compressingStream = new GZIPOutputStream(delegateOutputStream, blockSize, true) {{ def.setLevel(Deflater.BEST_COMPRESSION); }};
    }

    public GzipCompressingInputStream(InputStream delegate) throws IOException {
        this(delegate, DEFAULT_BLOCK_SIZE);
    }

    @Override
    protected OutputStream getCompressionOutputStream() {
        return this.compressingStream;
    }

    @Override
    protected void finishOutputStream() throws IOException {
        compressingStream.finish();
    }

    private static int maxCompressedLength(int blockSize) {
        return (int)(blockSize + 5 * (Math.floor(blockSize / 16383.) + 1)) + GZIP_TRAILER_SIZE + GZIP_HEADER_SIZE;
    }
}
