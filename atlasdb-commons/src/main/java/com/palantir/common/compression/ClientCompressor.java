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
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import com.google.common.collect.MoreCollectors;
import com.google.common.io.ByteStreams;
import com.palantir.common.base.Throwables;

import net.jpountz.lz4.LZ4BlockInputStream;

public enum ClientCompressor {
    GZIP(in -> {
        try {
                return new GzipCompressingInputStream(in);
            } catch (Exception exc) {
                throw new RuntimeException(exc);
            }
        }, in -> {
        try {
            return new GZIPInputStream(in);
        } catch (Exception exc) {
            throw new RuntimeException(exc);
        }
    }, GzipCompressingInputStream.GZIP_HEADER),
    LZ4(LZ4CompressingInputStream::new, LZ4BlockInputStream::new,
            new byte[] {'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'}),
    NONE(null, UnaryOperator.identity(), new byte[] {});

    private final UnaryOperator<InputStream> compressorCreator;
    private UnaryOperator<InputStream> decompressorCreator;
    public final byte[] magic;

    ClientCompressor(UnaryOperator<InputStream> compressorCreator, UnaryOperator<InputStream> decompressorCreator,
            byte[] magic) {
        this.compressorCreator = compressorCreator;
        this.decompressorCreator = decompressorCreator;
        this.magic = magic;
    }

    public InputStream getCompressor(InputStream stream) {
        return compressorCreator.apply(stream);
    }

    private boolean matchMagic(byte[] buffer, int bufferLen) {
        int i = 0;
        while (i < magic.length && i < bufferLen && magic[i] == buffer[i++]);
        return i >= magic.length && magic.length > 0;
    }

    /**
     * Method that takes a compressed stream and returns a decompressor stream. It will throw {@code
     * IllegalArgumentException} if more than one decompressor is detected,  a {@code NoSuchElementException} if no
     * compressor detected.
     */
    static InputStream getDecompressorStream(InputStream stream) throws IOException {
        BufferedInputStream buff = new BufferedInputStream(stream);
        List<ClientCompressor> compressors = Arrays.stream(ClientCompressor.values()).sorted(
                Comparator.comparingInt((ClientCompressor t) -> t.magic.length).reversed()
        ).collect(
                Collectors.toList());
        int maxLen = compressors.get(0).magic.length;
        buff.mark(maxLen);
        byte[] headerBuffer = new byte[maxLen];
        int len = ByteStreams.read(buff, headerBuffer, 0, maxLen);
        buff.reset();
        ClientCompressor compressor = compressors.stream().filter(
                t -> t.magic.length <= len && t.matchMagic(headerBuffer, len)).collect(MoreCollectors.onlyElement());
        return compressor.decompressorCreator.apply(buff);
    }
}
