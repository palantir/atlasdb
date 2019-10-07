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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import com.palantir.common.base.Throwables;

import net.jpountz.lz4.LZ4BlockInputStream;

public enum ClientCompressor {
    GZIP(GzipCompressingInputStream.class, GZIPInputStream.class, GzipCompressingInputStream.GZIP_HEADER),
    LZ4(LZ4CompressingInputStream.class, LZ4BlockInputStream.class,
            new byte[] {'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'}),
    NONE(null, null, new byte[] {});

    public final Class<InputStream> compressorClass;
    public final Class<InputStream> decompressorClass;
    public final byte[] magic;

    ClientCompressor(Class compressorClass, Class decompressorClass, byte[] magic) {
        this.compressorClass = compressorClass;
        this.decompressorClass = decompressorClass;
        this.magic = magic;
    }

    private static boolean matchMagic(ClientCompressor compressorType, byte[] buffer, int bufferLen) {
        byte[] signature = compressorType.magic;
        int i = 0;
        while (i < signature.length && i < bufferLen && signature[i] == buffer[i++]);
        return i >= signature.length && signature.length > 0;
    }

    static InputStream getDecompressorStream(InputStream stream) throws IOException {
        BufferedInputStream buff = new BufferedInputStream(stream);
        int maxLen = Arrays.stream(ClientCompressor.values()).max(
                Comparator.comparingInt(x -> x.magic.length)).get().magic.length;
        buff.mark(maxLen);
        byte[] headerBuffer = new byte[maxLen];
        int len = buff.read(headerBuffer);
        buff.reset();
        Optional<ClientCompressor> compressor = Arrays.stream(ClientCompressor.values()).filter(t -> matchMagic(t, headerBuffer, len)).findFirst();
        return compressor.map(t -> {
            try {
                return t.decompressorClass.getConstructor(InputStream.class).newInstance(buff);
            } catch (Exception exc) {
                throw new RuntimeException(exc);
            }
        }).orElseGet(() -> buff);
    }
}
