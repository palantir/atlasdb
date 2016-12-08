/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

public class LZ4DecompressingInputStreamTests {

    private static final byte[] COMPRESSIBLE_DATA_A = "aaaaaaaaaaaaaaa".getBytes();
    private static final byte[] COMPRESSED_DATA_A;
    private static final byte[] COMPRESSIBLE_DATA_B = "bbbbbbbbbbbbbbb".getBytes();
    private static final byte[] COMPRESSED_DATA_B;
    private static final LZ4FrameDescriptor CHECKSUM_FRAME_DESCRIPTOR = new LZ4FrameDescriptor(true, 5);

    static {
        COMPRESSED_DATA_A = LZ4Streams.compressor.compress(COMPRESSIBLE_DATA_A);
        COMPRESSED_DATA_B = LZ4Streams.compressor.compress(COMPRESSIBLE_DATA_B);
    }

    private byte[] compressedBuffer;
    private int bufferPosition;
    private InputStream decompressedStream;

    @Before
    public void before() throws IOException {
        compressedBuffer = new byte[512 * 1024];
        bufferPosition = 0;
        decompressedStream = new ByteArrayInputStream(new byte[0]);
    }

    @After
    public void after() throws IOException {
        decompressedStream.close();
    }

    @Test(expected = IllegalStateException.class)
    public void testFrameHeader_badMagicValue() throws IOException {
        writeFrameHeader(0, LZ4Streams.DEFAULT_FRAME_DESCRIPTOR);
        writeBlockSize(0);
        initializeDecompressedStream();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFrameHeader_invalidFrameDescriptor() throws IOException {
        writeInt(LZ4Streams.MAGIC_VALUE);
        byte[] frameDescriptorBytes = LZ4Streams.DEFAULT_FRAME_DESCRIPTOR.toByteArray();

        // Set an invalid compression block size
        // Toggle the 7th bit in the 2nd byte
        frameDescriptorBytes[1] ^= 0x40;
        System.arraycopy(frameDescriptorBytes, 0, compressedBuffer, bufferPosition, frameDescriptorBytes.length);

        writeBlockSize(0);
        initializeDecompressedStream();
    }

    @Test
    public void testEmptyStream() throws IOException {
        writeFrameHeader(LZ4Streams.DEFAULT_FRAME_DESCRIPTOR);
        writeBlockSize(0);
        initializeDecompressedStream();

        assertStreamIsEmpty();
    }

    @Test
    public void testSingleByte_uncompressed() throws IOException {
        writeFrameHeader(LZ4Streams.DEFAULT_FRAME_DESCRIPTOR);
        writeBlockSize(toUncompressedBlockSize(1));
        byte value = (byte) 42;
        byte[] data = new byte[] { value };
        writeBlock(data);
        writeBlockSize(0);
        initializeDecompressedStream();

        assertEquals(value, decompressedStream.read());
        assertStreamIsEmpty();
    }

    @Test
    public void testTwoBytes_uncompressed_readIndividually() throws IOException {
        writeFrameHeader(LZ4Streams.DEFAULT_FRAME_DESCRIPTOR);
        writeBlockSize(toUncompressedBlockSize(2));
        byte firstValue = (byte) 42;
        byte secondValue = (byte) 16;
        byte[] data = new byte[] { firstValue, secondValue };
        writeBlock(data);
        writeBlockSize(0);
        initializeDecompressedStream();

        assertEquals(firstValue, decompressedStream.read());
        assertEquals(secondValue, decompressedStream.read());
        assertStreamIsEmpty();
    }

    @Test
    public void testTwoBytes_uncompressed_readTogether() throws IOException {
        writeFrameHeader(LZ4Streams.DEFAULT_FRAME_DESCRIPTOR);
        writeBlockSize(toUncompressedBlockSize(2));
        byte firstValue = (byte) 42;
        byte secondValue = (byte) 16;
        byte[] data = new byte[] { firstValue, secondValue };
        writeBlock(data);
        writeBlockSize(0);
        initializeDecompressedStream();

        byte[] decompressedData = new byte[2];
        decompressedStream.read(decompressedData);

        assertArrayEquals(data, decompressedData);
        assertStreamIsEmpty();
    }

    @Test
    public void testTwoBytes_uncompressed_requestMoreBytesThanAvailable() throws IOException {
        writeFrameHeader(LZ4Streams.DEFAULT_FRAME_DESCRIPTOR);
        writeBlockSize(toUncompressedBlockSize(2));
        byte firstValue = (byte) 42;
        byte secondValue = (byte) 16;
        byte[] data = new byte[] { firstValue, secondValue };
        writeBlock(data);
        writeBlockSize(0);
        initializeDecompressedStream();

        byte[] decompressedData = new byte[100];
        int bytesRead = decompressedStream.read(decompressedData);

        assertEquals(data.length, bytesRead);
        assertArrayEquals(data, Arrays.copyOf(decompressedData, bytesRead));
        assertStreamIsEmpty();
    }

    @Test
    public void testSingleBlock_compressible() throws IOException {
        writeFrameHeader(LZ4Streams.DEFAULT_FRAME_DESCRIPTOR);
        writeBlockSize(COMPRESSED_DATA_A.length);
        writeBlock(COMPRESSED_DATA_A);
        writeBlockSize(0);
        initializeDecompressedStream();

        byte[] decompressedData = new byte[COMPRESSIBLE_DATA_A.length];
        int bytesRead = decompressedStream.read(decompressedData);

        assertEquals(COMPRESSIBLE_DATA_A.length, bytesRead);
        assertArrayEquals(COMPRESSIBLE_DATA_A, decompressedData);
        assertStreamIsEmpty();
    }

    @Test
    public void testMultiBlock_uncompressed() throws IOException {
        writeFrameHeader(LZ4Streams.DEFAULT_FRAME_DESCRIPTOR);
        writeBlockSize(toUncompressedBlockSize(1));
        byte firstValue = (byte) 42;
        writeBlock(new byte[] { firstValue });
        writeBlockSize(toUncompressedBlockSize(1));
        byte secondValue = (byte) 16;
        writeBlock(new byte[] { secondValue });
        writeBlockSize(0);
        initializeDecompressedStream();

        assertEquals(firstValue, decompressedStream.read());
        assertEquals(secondValue, decompressedStream.read());
        assertStreamIsEmpty();
    }

    @Test
    public void testMultiBlock_compressed() throws IOException {
        writeFrameHeader(LZ4Streams.DEFAULT_FRAME_DESCRIPTOR);
        writeBlockSize(COMPRESSED_DATA_A.length);
        writeBlock(COMPRESSED_DATA_A);
        writeBlockSize(COMPRESSED_DATA_B.length);
        writeBlock(COMPRESSED_DATA_B);
        writeBlockSize(0);
        initializeDecompressedStream();

        int decompressedDataLength = COMPRESSIBLE_DATA_A.length + COMPRESSIBLE_DATA_B.length;
        byte[] decompressedData = new byte[decompressedDataLength];
        int bytesRead = decompressedStream.read(decompressedData);

        byte[] expectedData = new byte[decompressedDataLength];
        System.arraycopy(COMPRESSIBLE_DATA_A, 0, expectedData, 0, COMPRESSIBLE_DATA_A.length);
        System.arraycopy(COMPRESSIBLE_DATA_B, 0, expectedData, COMPRESSIBLE_DATA_A.length, COMPRESSIBLE_DATA_B.length);

        assertEquals(decompressedDataLength, bytesRead);
        assertArrayEquals(expectedData, decompressedData);
        assertStreamIsEmpty();
    }

    @Test
    public void testMultiBlock_mixed() throws IOException {
        writeFrameHeader(LZ4Streams.DEFAULT_FRAME_DESCRIPTOR);
        writeBlockSize(COMPRESSED_DATA_A.length);
        writeBlock(COMPRESSED_DATA_A);
        byte firstValue = (byte) 42;
        writeBlockSize(toUncompressedBlockSize(1));
        writeBlock(new byte[] { firstValue });
        writeBlockSize(0);
        initializeDecompressedStream();

        int decompressedDataLength = COMPRESSIBLE_DATA_A.length + 1;
        byte[] decompressedData = new byte[decompressedDataLength];
        int bytesRead = decompressedStream.read(decompressedData);

        byte[] expectedData = new byte[decompressedDataLength];
        System.arraycopy(COMPRESSIBLE_DATA_A, 0, expectedData, 0, COMPRESSIBLE_DATA_A.length);
        expectedData[COMPRESSIBLE_DATA_A.length] = firstValue;

        assertEquals(decompressedDataLength, bytesRead);
        assertArrayEquals(expectedData, decompressedData);
        assertStreamIsEmpty();
    }

    @Test
    public void testMultiBlock_mixed_correctHash() throws IOException {
        byte[] firstValue = new byte[] { 42 };
        StreamingXXHash32 hash = XXHashFactory.fastestInstance().newStreamingHash32(0);
        hash.update(COMPRESSIBLE_DATA_A, 0, COMPRESSIBLE_DATA_A.length);
        hash.update(firstValue, 0, 1);
        int expectedHashValue = hash.getValue();

        writeFrameHeader(CHECKSUM_FRAME_DESCRIPTOR);
        writeBlockSize(COMPRESSED_DATA_A.length);
        writeBlock(COMPRESSED_DATA_A);
        writeBlockSize(toUncompressedBlockSize(1));
        writeBlock(firstValue);
        writeBlockSize(0);
        writeHashValue(expectedHashValue);
        initializeDecompressedStream();

        int decompressedDataLength = COMPRESSIBLE_DATA_A.length + 1;
        byte[] decompressedData = new byte[decompressedDataLength];
        // Fully draining the decompressing stream triggers the checksum comparison
        int bytesRead = decompressedStream.read(decompressedData);

        assertEquals(decompressedDataLength, bytesRead);
        assertStreamIsEmpty();
    }

    @Test(expected = IllegalStateException.class)
    public void testMultiBlock_mixed_incorrectHash() throws IOException {
        writeFrameHeader(CHECKSUM_FRAME_DESCRIPTOR);
        writeBlockSize(COMPRESSED_DATA_A.length);
        writeBlock(COMPRESSED_DATA_A);
        writeBlockSize(toUncompressedBlockSize(1));
        writeBlock(new byte[] { 42 });
        writeBlockSize(0);
        writeHashValue(0);
        initializeDecompressedStream();

        int decompressedDataLength = COMPRESSIBLE_DATA_A.length + 1;
        byte[] decompressedData = new byte[decompressedDataLength];
        // Fully draining the decompressing stream triggers the checksum comparison
        decompressedStream.read(decompressedData);
    }

    @Test
    public void testMultiBlock_compressedByCompressingStream_withChecksum() throws IOException {
        byte[] uncompressedData = new byte[512 * 1024];
        Arrays.fill(uncompressedData, (byte) 0xFF);
        InputStream compressedStream = new LZ4CompressingInputStream(new ByteArrayInputStream(uncompressedData),
                CHECKSUM_FRAME_DESCRIPTOR);
        decompressedStream = new LZ4DecompressingInputStream(compressedStream);

        byte[] decompressedData = new byte[512 * 1024];
        int bytesRead = decompressedStream.read(decompressedData);

        assertEquals(uncompressedData.length, bytesRead);
        assertArrayEquals(uncompressedData, decompressedData);
        assertStreamIsEmpty();
    }

    private void writeFrameHeader(LZ4FrameDescriptor frameDescriptor) {
        writeFrameHeader(LZ4Streams.MAGIC_VALUE, frameDescriptor);
    }

    private void writeFrameHeader(int magicValue, LZ4FrameDescriptor frameDescriptor) {
        writeInt(magicValue);
        byte[] frameDescriptorBytes = frameDescriptor.toByteArray();
        System.arraycopy(frameDescriptorBytes, 0, compressedBuffer, bufferPosition,
                LZ4Streams.FRAME_DESCRIPTOR_LENGTH);
        bufferPosition += LZ4Streams.FRAME_DESCRIPTOR_LENGTH;
    }

    private void writeInt(int value) {
        byte[] valueBytes = LZ4Streams.intToLittleEndianBytes(value);
        System.arraycopy(valueBytes, 0, compressedBuffer, bufferPosition, 4);
        bufferPosition += 4;
    }

    private void writeBlockSize(int blockSize) {
        writeInt(blockSize);
    }

    private void writeBlock(byte[] blockBytes) {
        System.arraycopy(blockBytes, 0, compressedBuffer, bufferPosition, blockBytes.length);
        bufferPosition += blockBytes.length;
    }

    private void writeHashValue(int hashValue) {
        writeInt(hashValue);
    }

    private void initializeDecompressedStream() throws IOException {
        decompressedStream = new LZ4DecompressingInputStream(new ByteArrayInputStream(compressedBuffer));
    }

    private static int toUncompressedBlockSize(int blockSize) {
        return blockSize ^ 0x80000000;
    }

    private void assertStreamIsEmpty() throws IOException {
        assertEquals(-1, decompressedStream.read());
    }

}
