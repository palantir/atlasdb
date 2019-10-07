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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import com.google.common.io.ByteStreams;

public abstract class AbstractCompressionTests {

    private static final byte SINGLE_VALUE = 42;
    private static final int BLOCK_SIZE = 1 << 16; // 64 KB

    protected ByteArrayInputStream uncompressedStream;
    protected InputStream compressingStream;
    protected InputStream decompressingStream;

    @After
    public void close() throws IOException {
        decompressingStream.close();
    }


    @Test
    public void testEmptyStream() throws Exception {
        initializeStreams(new byte[0]);
        assertStreamIsEmpty(decompressingStream);
    }

    @Test
    public void testSingleCharacterStream() throws Exception {
        testStream_incompressible(1); // 1 byte input will always be incompressible
    }

    @Test
    public void testSingleCharacterStream_singleByteRead() throws Exception {
        byte[] uncompressedData = new byte[] { SINGLE_VALUE };
        initializeStreams(uncompressedData);
        int value = decompressingStream.read();

        assertEquals(uncompressedData[0] & 0xFF, value);
        assertStreamIsEmpty(decompressingStream);
    }

    @Test
    public void testSingleBlock_compressible() throws Exception {
        testStream_compressible(BLOCK_SIZE);
    }

    @Test
    public void testSingleBlock_incompressible() throws Exception {
        testStream_incompressible(BLOCK_SIZE);
    }

    @Test
    public void testMultiBlock_compressible() throws Exception {
        testStream_compressible(16 * BLOCK_SIZE);
    }

    @Test
    public void testMultiBlock_incompressible() throws Exception {
        testStream_incompressible(16 * BLOCK_SIZE);
    }

    @Test
    public void testMultiBlock_singleByteReads() throws Exception {
        byte[] uncompressedData = new byte[16 * BLOCK_SIZE];
        fillWithIncompressibleData(uncompressedData);
        initializeStreams(uncompressedData);

        for (int i = 0; i < uncompressedData.length; ++i) {
            int value = decompressingStream.read();
            assertEquals(uncompressedData[i] & 0xFF, value);
        }
        assertStreamIsEmpty(decompressingStream);
    }

    @Test
    public void testMultiBlock_readPastEnd() throws Exception {
        byte[] uncompressedData = new byte[16 * BLOCK_SIZE];
        fillWithCompressibleData(uncompressedData);
        initializeStreams(uncompressedData);

        byte[] decompressedData = new byte[17 * BLOCK_SIZE];
        int bytesRead = ByteStreams.read(decompressingStream, decompressedData, 0, decompressedData.length);
        assertEquals(uncompressedData.length, bytesRead);
        assertArrayEquals(uncompressedData, Arrays.copyOf(decompressedData, bytesRead));
    }

    private void testStream_compressible(int streamSize) throws Exception {
        byte[] uncompressedData = new byte[streamSize];
        fillWithCompressibleData(uncompressedData);
        initializeStreams(uncompressedData);
        verifyStreamContents(uncompressedData);
    }

    private void testStream_incompressible(int streamSize) throws Exception {
        byte[] uncompressedData = new byte[streamSize];
        fillWithIncompressibleData(uncompressedData);
        initializeStreams(uncompressedData);
        verifyStreamContents(uncompressedData);
    }

    protected abstract void initializeCompressStreams() throws Exception;

    private void initializeStreams(byte[] uncompressedData) throws Exception {
        uncompressedStream = new ByteArrayInputStream(uncompressedData);
        initializeCompressStreams();
    }



    private void fillWithCompressibleData(byte[] data) {
        Arrays.fill(data, SINGLE_VALUE);
    }

    private void fillWithIncompressibleData(byte[] data) {
        new Random(0).nextBytes(data);
    }

    private void verifyStreamContents(byte[] uncompressedData) throws IOException {
        byte[] decompressedData = new byte[uncompressedData.length];
        ByteStreams.read(decompressingStream, decompressedData, 0, decompressedData.length);
        assertArrayEquals(uncompressedData, decompressedData);
        assertStreamIsEmpty(decompressingStream);
    }

    private void assertStreamIsEmpty(InputStream stream) throws IOException {
        assertEquals(-1, stream.read());
    }
}
