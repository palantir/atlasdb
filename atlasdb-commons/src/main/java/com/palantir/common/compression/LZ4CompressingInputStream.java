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
public final class LZ4CompressingInputStream extends AbstractCompressingInputStream {

    private final LZ4BlockOutputStream compressingStream;
    private static LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    private static final int DEFAULT_SEED = 0x9747b28c;
    private static final int DEFAULT_BLOCK_SIZE = 1 << 16; // 64 KB
    private static final int LZ4_HEADER_SIZE = 21;

    public LZ4CompressingInputStream(InputStream delegate) throws IOException {
        this(delegate, DEFAULT_BLOCK_SIZE);
    }

    public LZ4CompressingInputStream(InputStream delegate, boolean highCompression) throws IOException {
        this(delegate, DEFAULT_BLOCK_SIZE);
        if (highCompression)
            compressor = LZ4Factory.fastestInstance().highCompressor();
    }

    public LZ4CompressingInputStream(InputStream delegate, int blockSize) throws IOException {
        super(delegate, blockSize, LZ4_HEADER_SIZE + compressor.maxCompressedLength(blockSize));
        Checksum checksum = XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum();
        OutputStream delegateOutputStream = new InternalByteArrayOutputStream();
        this.compressingStream = new LZ4BlockOutputStream(delegateOutputStream, blockSize, compressor, checksum, true);
    }

    @Override
    protected OutputStream getCompressionOutputStream() {
        return this.compressingStream;
    }

    @Override
    protected void finishOutputStream() throws IOException {
        compressingStream.finish();
    }

}
