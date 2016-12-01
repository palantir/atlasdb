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

public class LZ4CompressingInputStreamTests {

    private static final String COMPRESSIBLE_STRING = "aaaaaaaaaaaaaaa";
    private static final byte[] COMPRESSIBLE_DATA = COMPRESSIBLE_STRING.getBytes();
    private static final String INCOMPRESSIBLE_STRING = "a";
    private static final byte[] INCOMPRESSIBLE_DATA = INCOMPRESSIBLE_STRING.getBytes();
    private static final LZ4FrameDescriptor CHECKSUM_FRAME_DESCRIPTOR = new LZ4FrameDescriptor(true, 5);

    private InputStream compressibleStream;
    private InputStream incompressibleStream;
    private InputStream emptyStream;
    private LZ4CompressingInputStream compressedStream;

    @Before
    public void before() {
        compressibleStream = new ByteArrayInputStream(COMPRESSIBLE_DATA);
        incompressibleStream = new ByteArrayInputStream(INCOMPRESSIBLE_DATA);
        emptyStream = new ByteArrayInputStream(new byte[0]);
    }

    @After
    public void after() throws IOException {
        compressibleStream.close();
        incompressibleStream.close();
        emptyStream.close();
        compressedStream.close();
    }

    @Test
    public void testFrameHeaderFormat_defaultFrameDescriptor() throws IOException {
        compressedStream = new LZ4CompressingInputStream(emptyStream);
        byte[] frameHeader = new byte[LZ4Streams.FRAME_HEADER_LENGTH];
        int bytesRead = compressedStream.read(frameHeader);
        int magicValue = LZ4Streams.intFromLittleEndianBytes(frameHeader, 0);
        LZ4FrameDescriptor frameDescriptor = LZ4FrameDescriptor.fromByteArray(
                Arrays.copyOfRange(frameHeader, LZ4Streams.MAGIC_LENGTH, LZ4Streams.FRAME_HEADER_LENGTH));

        assertEquals(LZ4Streams.FRAME_HEADER_LENGTH, bytesRead);
        assertEquals(LZ4Streams.MAGIC_VALUE, magicValue);
        assertEquals(LZ4Streams.DEFAULT_FRAME_DESCRIPTOR, frameDescriptor);
    }

    @Test
    public void testFrameHeaderFormat_customFrameDescriptor() throws IOException {
        compressedStream = new LZ4CompressingInputStream(emptyStream, CHECKSUM_FRAME_DESCRIPTOR);
        byte[] frameHeader = new byte[LZ4Streams.FRAME_HEADER_LENGTH];
        int bytesRead = compressedStream.read(frameHeader);
        int magicValue = LZ4Streams.intFromLittleEndianBytes(frameHeader, 0);
        LZ4FrameDescriptor frameDescriptor = LZ4FrameDescriptor.fromByteArray(
                Arrays.copyOfRange(frameHeader, LZ4Streams.MAGIC_LENGTH, LZ4Streams.FRAME_HEADER_LENGTH));

        assertEquals(LZ4Streams.FRAME_HEADER_LENGTH, bytesRead);
        assertEquals(LZ4Streams.MAGIC_VALUE, magicValue);
        assertEquals(CHECKSUM_FRAME_DESCRIPTOR, frameDescriptor);
    }

    @Test
    public void testEmptyStream() throws IOException {
        compressedStream = new LZ4CompressingInputStream(emptyStream);
        byte[] compressedContents = new byte[100];
        int streamSize = compressedStream.read(compressedContents);
        int blockSize = LZ4Streams.intFromLittleEndianBytes(compressedContents, streamSize - 4);

        assertEquals(LZ4Streams.FRAME_HEADER_LENGTH + LZ4Streams.BLOCK_HEADER_LENGTH, streamSize);
        assertEquals(0, blockSize);
    }

    @Test
    public void testEmptyStream_withContentChecksum() throws IOException {
        compressedStream = new LZ4CompressingInputStream(emptyStream, CHECKSUM_FRAME_DESCRIPTOR);
        byte[] compressedContents = new byte[100];
        int streamSize = compressedStream.read(compressedContents);
        int blockSize = LZ4Streams.intFromLittleEndianBytes(compressedContents, streamSize - 8);
        int hashValue = LZ4Streams.intFromLittleEndianBytes(compressedContents, streamSize - 4);
        int expectedHash = XXHashFactory.fastestInstance().newStreamingHash32(0).getValue();

        assertEquals(LZ4Streams.FRAME_HEADER_LENGTH + LZ4Streams.BLOCK_HEADER_LENGTH + 4, streamSize);
        assertEquals(0, blockSize);
        assertEquals(expectedHash, hashValue);
    }

    @Test
    public void testSingleCompressedBlock() throws IOException {
        compressedStream = new LZ4CompressingInputStream(compressibleStream, CHECKSUM_FRAME_DESCRIPTOR);
        skipFrameHeader();
        byte[] data = new byte[100];
        assertEquals(4, compressedStream.read(data, 0, 4));
        int blockSize = LZ4Streams.intFromLittleEndianBytes(data, 0);
        int bytesRead = compressedStream.read(data, 0, blockSize);
        byte[] compressedData = Arrays.copyOf(data, bytesRead);
        assertEquals(4, compressedStream.read(data, 0, 4));
        int finalBlockSize = LZ4Streams.intFromLittleEndianBytes(data, 0);
        byte[] expectedCompressedData = LZ4Streams.compressor.compress(COMPRESSIBLE_DATA);

        StreamingXXHash32 hash = XXHashFactory.fastestInstance().newStreamingHash32(0);
        hash.update(COMPRESSIBLE_DATA, 0, COMPRESSIBLE_DATA.length);
        int expectedHash = hash.getValue();
        assertEquals(4, compressedStream.read(data, 0, 4));
        int actualHash = LZ4Streams.intFromLittleEndianBytes(data, 0);

        assertEquals(expectedCompressedData.length, blockSize);
        assertArrayEquals(expectedCompressedData, compressedData);
        assertEquals(0, finalBlockSize);
        assertEquals(expectedHash, actualHash);
        assertEquals(-1, compressedStream.read());
    }

    @Test
    public void testSingleCompressedBlock_singleByteRead() throws IOException {
        compressedStream = new LZ4CompressingInputStream(compressibleStream);
        skipFrameHeader();
        byte[] data = new byte[100];
        assertEquals(4, compressedStream.read(data, 0, 4));

        int value = compressedStream.read();
        byte[] expectedCompressedData = LZ4Streams.compressor.compress(COMPRESSIBLE_DATA);

        assertEquals(expectedCompressedData[0], value);
    }

    @Test
    public void testSingleUncompressedBlock() throws IOException {
        compressedStream = new LZ4CompressingInputStream(incompressibleStream, CHECKSUM_FRAME_DESCRIPTOR);
        skipFrameHeader();
        byte[] data = new byte[100];
        assertEquals(4, compressedStream.read(data, 0, 4));
        int blockSize = LZ4Streams.intFromLittleEndianBytes(data, 0);
        int bytesRead = compressedStream.read(data, 0, INCOMPRESSIBLE_DATA.length);
        byte[] streamData = Arrays.copyOf(data, bytesRead);
        assertEquals(4, compressedStream.read(data, 0, 4));
        int finalBlockSize = LZ4Streams.intFromLittleEndianBytes(data, 0);

        StreamingXXHash32 hash = XXHashFactory.fastestInstance().newStreamingHash32(0);
        hash.update(INCOMPRESSIBLE_DATA, 0, INCOMPRESSIBLE_DATA.length);
        int expectedHash = hash.getValue();
        assertEquals(4, compressedStream.read(data, 0, 4));
        int actualHash = LZ4Streams.intFromLittleEndianBytes(data, 0);

        int expectedBlockSize = INCOMPRESSIBLE_DATA.length ^ 0x80000000;
        int actualBlockSize = blockSize ^ 0x80000000;

        assertEquals(expectedBlockSize, blockSize);
        assertEquals(INCOMPRESSIBLE_DATA.length, actualBlockSize);
        assertArrayEquals(INCOMPRESSIBLE_DATA, streamData);
        assertEquals(0, finalBlockSize);
        assertEquals(expectedHash, actualHash);
        assertEquals(-1, compressedStream.read());
    }

    @Test
    public void testMultiBlock() throws IOException {
        // 512 KB = 2 compressed blocks
        byte[] uncompressedData = new byte[512 * 1024];
        Arrays.fill(uncompressedData, (byte) 0xFF);
        StreamingXXHash32 hash = XXHashFactory.fastestInstance().newStreamingHash32(0);
        hash.update(uncompressedData, 0, 256 * 1024);
        hash.update(uncompressedData, 256 * 1024, 256 * 1024);
        int expectedHash = hash.getValue();
        byte[] compressedBlock = LZ4Streams.compressor.compress(Arrays.copyOf(uncompressedData, 256 * 1024));

        ByteArrayInputStream inputStream = new ByteArrayInputStream(uncompressedData);
        compressedStream = new LZ4CompressingInputStream(inputStream, CHECKSUM_FRAME_DESCRIPTOR);
        skipFrameHeader();
        byte[] data = new byte[LZ4Streams.getCompressedBufferSize(CHECKSUM_FRAME_DESCRIPTOR)];

        // Read first block
        assertEquals(4, compressedStream.read(data, 0, 4));
        int firstBlockSize = LZ4Streams.intFromLittleEndianBytes(data, 0);
        int firstBlockBytesRead = compressedStream.read(data, 0, firstBlockSize);
        byte[] firstBlockData = Arrays.copyOf(data, firstBlockBytesRead);

        // Read second block
        assertEquals(4, compressedStream.read(data, 0, 4));
        int secondBlockSize = LZ4Streams.intFromLittleEndianBytes(data, 0);
        int secondBlockBytesRead = compressedStream.read(data, 0, secondBlockSize);
        byte[] secondBlockData = Arrays.copyOf(data, secondBlockBytesRead);

        // Read final block size and footer
        assertEquals(4, compressedStream.read(data, 0, 4));
        int finalBlockSize = LZ4Streams.intFromLittleEndianBytes(data, 0);
        assertEquals(4, compressedStream.read(data, 0, 4));
        int actualHash = LZ4Streams.intFromLittleEndianBytes(data, 0);

        assertEquals(compressedBlock.length, firstBlockSize);
        assertEquals(compressedBlock.length, secondBlockSize);
        assertEquals(0, finalBlockSize);
        assertArrayEquals(compressedBlock, firstBlockData);
        assertArrayEquals(compressedBlock, secondBlockData);
        assertEquals(expectedHash, actualHash);
        assertEquals(-1, compressedStream.read());
    }

    @Test
    public void testMultiBlock_readMultipleBlocksAtOnce() throws IOException {
        // 512 KB = 2 compressed blocks
        byte[] uncompressedData = new byte[512 * 1024];
        Arrays.fill(uncompressedData, (byte) 0xFF);
        byte[] compressedBlock = LZ4Streams.compressor.compress(Arrays.copyOf(uncompressedData, 256 * 1024));

        ByteArrayInputStream inputStream = new ByteArrayInputStream(uncompressedData);
        compressedStream = new LZ4CompressingInputStream(inputStream, CHECKSUM_FRAME_DESCRIPTOR);
        int expectedStreamSize = LZ4Streams.FRAME_HEADER_LENGTH
                + 3 * LZ4Streams.BLOCK_HEADER_LENGTH
                + 2 * compressedBlock.length
                + 4; // checksum
        byte[] data = new byte[expectedStreamSize];
        int bytesRead = compressedStream.read(data);

        assertEquals(expectedStreamSize, bytesRead);
        assertEquals(-1, compressedStream.read());
    }

    private void skipFrameHeader() throws IOException {
        byte[] header = new byte[LZ4Streams.FRAME_HEADER_LENGTH];
        int bytesRead = compressedStream.read(header);
        assertEquals(LZ4Streams.FRAME_HEADER_LENGTH, bytesRead);
    }

    // Failure edge cases on read

}
