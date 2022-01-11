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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StreamCompressionTests {
    private static final StreamCompression GZIP = StreamCompression.GZIP;
    private static final StreamCompression LZ4 = StreamCompression.LZ4;
    private static final StreamCompression NONE = StreamCompression.NONE;

    private static final byte SINGLE_VALUE = 42;
    private static final int BLOCK_SIZE = 1 << 16; // 64 KB

    private InputStream compressingStream;
    private InputStream decompressingStream;

    private final StreamCompression compression;

    public StreamCompressionTests(StreamCompression compression) {
        this.compression = compression;
    }

    @Parameterized.Parameters(name = "{index} {0} compression")
    public static Object[] parameters() {
        return StreamCompression.values();
    }

    @After
    public void close() throws IOException {
        if (decompressingStream != null) {
            decompressingStream.close();
        } else if (compressingStream != null) {
            compressingStream.close();
        }
    }

    @Test
    public void testUncompressed_doesNotDecompressEvenIfDataCompressed() throws IOException {
        byte[] data = new byte[1_000_000];
        fillWithIncompressibleData(data);
        assertThat(ByteStreams.toByteArray(
                        GZIP.decompress(NONE.decompress(GZIP.compress(new ByteArrayInputStream(data))))))
                .isEqualTo(data);
    }

    @Test
    public void testCanDecompressGzipAsLz4() throws IOException {
        byte[] data = new byte[1_000_000];
        fillWithIncompressibleData(data);
        assertThat(ByteStreams.toByteArray(LZ4.decompress(GZIP.compress(new ByteArrayInputStream(data)))))
                .isEqualTo(data);
    }

    @Test
    public void testEmptyStream() throws IOException {
        initializeStreams(new byte[0]);
        assertStreamIsEmpty(decompressingStream);
    }

    @Test
    public void testSingleCharacterStream() throws IOException {
        testStream_incompressible(1); // 1 byte input will always be incompressible
    }

    @Test
    public void testSingleCharacterStream_singleByteRead() throws IOException {
        byte[] uncompressedData = new byte[] {SINGLE_VALUE};
        initializeStreams(uncompressedData);
        int value = decompressingStream.read();

        assertThat(value).isEqualTo(uncompressedData[0] & 0xFF);
        assertStreamIsEmpty(decompressingStream);
    }

    @Test
    public void testSingleBlock_compressible() throws IOException {
        testStream_compressible(BLOCK_SIZE);
    }

    @Test
    public void testSingleBlock_incompressible() throws IOException {
        testStream_incompressible(BLOCK_SIZE);
    }

    @Test
    public void testMultiBlock_compressible() throws IOException {
        testStream_compressible(16 * BLOCK_SIZE);
    }

    @Test
    public void testMultiBlock_incompressible() throws IOException {
        testStream_incompressible(16 * BLOCK_SIZE);
    }

    @Test
    public void testMultiBlock_singleByteReads() throws IOException {
        byte[] uncompressedData = new byte[16 * BLOCK_SIZE];
        fillWithIncompressibleData(uncompressedData);
        initializeStreams(uncompressedData);

        for (byte uncompressedDatum : uncompressedData) {
            int value = decompressingStream.read();
            assertThat(value).isEqualTo(uncompressedDatum & 0xFF);
        }
        assertStreamIsEmpty(decompressingStream);
    }

    @Test
    public void testMultiBlock_readPastEnd() throws IOException {
        byte[] uncompressedData = new byte[16 * BLOCK_SIZE];
        fillWithCompressibleData(uncompressedData);
        initializeStreams(uncompressedData);

        byte[] decompressedData = new byte[17 * BLOCK_SIZE];
        int bytesRead = ByteStreams.read(decompressingStream, decompressedData, 0, decompressedData.length);
        assertThat(bytesRead).isEqualTo(uncompressedData.length);
        assertThat(Arrays.copyOf(decompressedData, bytesRead)).isEqualTo(uncompressedData);
    }

    private void testStream_compressible(int streamSize) throws IOException {
        byte[] uncompressedData = new byte[streamSize];
        fillWithCompressibleData(uncompressedData);
        initializeStreams(uncompressedData);
        verifyStreamContents(uncompressedData);
    }

    private void testStream_incompressible(int streamSize) throws IOException {
        byte[] uncompressedData = new byte[streamSize];
        fillWithIncompressibleData(uncompressedData);
        initializeStreams(uncompressedData);
        verifyStreamContents(uncompressedData);
    }

    private void initializeStreams(byte[] uncompressedData) {
        ByteArrayInputStream uncompressedStream = new ByteArrayInputStream(uncompressedData);
        compressingStream = compression.compress(uncompressedStream);
        decompressingStream = compression.decompress(compressingStream);
    }

    private static void fillWithCompressibleData(byte[] data) {
        Arrays.fill(data, SINGLE_VALUE);
    }

    private static void fillWithIncompressibleData(byte[] data) {
        new Random(0).nextBytes(data);
    }

    private void verifyStreamContents(byte[] uncompressedData) throws IOException {
        byte[] decompressedData = new byte[uncompressedData.length];
        ByteStreams.read(decompressingStream, decompressedData, 0, decompressedData.length);
        assertThat(decompressedData).isEqualTo(uncompressedData);
        assertStreamIsEmpty(decompressingStream);
    }

    private static void assertStreamIsEmpty(InputStream stream) throws IOException {
        assertThat(stream.read()).isEqualTo(-1);
    }
}
