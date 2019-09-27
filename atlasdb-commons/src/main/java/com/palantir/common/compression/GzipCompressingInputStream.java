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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterInputStream;

import com.google.common.io.CountingInputStream;


public class GzipCompressingInputStream extends SequenceInputStream {
    public GzipCompressingInputStream(InputStream in) throws IOException {
        this(in, 512);
    }

    public GzipCompressingInputStream(InputStream in, int bufferSize) throws IOException {
        super(new GzipStreamEnumeration(in, bufferSize));
    }

    protected static class GzipStreamEnumeration implements Enumeration<InputStream> {
        private static final int GZIP_MAGIC = 0x8b1f;
        private static final int DEFAULT_DEFLATER_BUFFER_SIZE = 512;
        private static final byte[] GZIP_HEADER = new byte[] {
                (byte) GZIP_MAGIC,        // Magic number (short)
                (byte) (GZIP_MAGIC >> 8),  // Magic number (short)
                Deflater.DEFLATED,        // Compression method (CM)
                0,                        // Flags (FLG)
                0,                        // Modification time MTIME (int)
                0,                        // Modification time MTIME (int)
                0,                        // Modification time MTIME (int)
                0,                        // Modification time MTIME (int)
                0,                        // Extra flags (XFLG)
                0                         // Operating system (OS)
        };
        private final InputStream in;
        private final int bufferSize;
        private final Iterator<Supplier<InputStream>> streamSupplierIt;
        private CheckedInputStream checkedInputStream;
        private DeflaterInputStream contentStream;
        private CountingInputStream counting;

        public GzipStreamEnumeration(InputStream in) {
            this(in, DEFAULT_DEFLATER_BUFFER_SIZE);
        }

        public GzipStreamEnumeration(InputStream in, int bufferSize) {
            this.in = in;
            this.bufferSize = bufferSize;
            streamSupplierIt = Arrays.<Supplier<InputStream>>asList(() -> createHeaderStream(),
                    () -> createContentStream(),
                    () -> createTrailerStream()
            ).iterator();
        }

        public boolean hasMoreElements() {
            return streamSupplierIt.hasNext();
        }

        public InputStream nextElement() {
            if (hasMoreElements()) {
                return streamSupplierIt.next().get();
            } else {
                return null;
            }
        }

        private InputStream createHeaderStream() {
            return new ByteArrayInputStream(GZIP_HEADER);
        }

        private InputStream createContentStream() {
            counting = new CountingInputStream(in);
            CRC32 crc = new CRC32();
            checkedInputStream = new CheckedInputStream(counting, crc);
            contentStream = new DeflaterInputStream(checkedInputStream,
                    new Deflater(Deflater.DEFAULT_COMPRESSION, true), bufferSize);
            return contentStream;
        }

        private InputStream createTrailerStream() {
            long checksum = checkedInputStream.getChecksum().getValue();
            long count = counting.getCount();
            byte[] trailer = new byte[Integer.BYTES * 2];
            ByteBuffer buffer = ByteBuffer.wrap(trailer).order(ByteOrder.LITTLE_ENDIAN);
            buffer.putInt((int)(checksum & 0xffffffffL));
            buffer.putInt((int) count);
            return new ByteArrayInputStream(trailer);

        }
    }

}