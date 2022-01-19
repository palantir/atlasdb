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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.CountingInputStream;
import com.google.common.primitives.Shorts;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterInputStream;

public final class GzipCompressingInputStream {
    private static final int GZIP_MAGIC = 0x8b1f;
    private static final byte[] GZIP_HEADER = new byte[] {
        (byte) GZIP_MAGIC, // Magic number (short)
        (byte) (GZIP_MAGIC >> 8), // Magic number (short)
        Deflater.DEFLATED, // Compression method (CM)
        0, // Flags (FLG)
        0, // Modification time MTIME (int)
        0, // Modification time MTIME (int)
        0, // Modification time MTIME (int)
        0, // Modification time MTIME (int)
        0, // Extra flags (XFLG)
        0 // Operating system (OS)
    };

    public static byte[] getMagicPrefix() {
        return Arrays.copyOf(GZIP_HEADER, 2);
    }

    /**
     * Wraps a stream with GZIP compression using default compression level and reasonable default buffer size.
     *
     * @param uncompressed uncompressed stream to wrap
     * @return GZIP compressed stream
     */
    public static InputStream compress(InputStream uncompressed) {
        return compress(
                uncompressed, Shorts.checkedCast(Deflater.DEFAULT_COMPRESSION), StreamCompression.DEFAULT_BLOCK_SIZE);
    }

    /**
     * Wraps a stream with GZIP compression.
     *
     * @param uncompressed uncompressed stream to wrap
     * @param compressionLevel compression level (1 to 9), see {@link Deflater}
     * @param bufferSize deflation buffer size
     * @return GZIP compressed stream
     */
    public static InputStream compress(InputStream uncompressed, short compressionLevel, int bufferSize) {
        InputStream header = createHeaderStream();
        CountingInputStream counting = new CountingInputStream(uncompressed);
        CRC32 crc = new CRC32();
        CheckedInputStream checked = new CheckedInputStream(counting, crc);
        InputStream content = new DeflaterInputStream(checked, new Deflater(compressionLevel, true), bufferSize);
        List<Supplier<InputStream>> allStreams =
                ImmutableList.of(() -> header, () -> content, () -> trailerStream(counting.getCount(), crc));
        return new SequenceInputStream(Collections.enumeration(Lists.transform(allStreams, Supplier::get)));
    }

    private static InputStream trailerStream(long count, CRC32 crc) {
        long checksum = crc.getValue();
        byte[] trailer = new byte[Integer.BYTES * 2];
        ByteBuffer buffer = ByteBuffer.wrap(trailer).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt((int) (checksum & 0xffffffffL));
        buffer.putInt((int) count);
        return new ByteArrayInputStream(trailer);
    }

    private static InputStream createHeaderStream() {
        return new ByteArrayInputStream(GZIP_HEADER);
    }
}
