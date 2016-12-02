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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.Test;

import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

public class LZ4CompressingInputStreamTests {

    private static final byte[] COMPRESSIBLE_DATA = "aaaaaaaaaaaaaaa".getBytes();
    private static final byte[] INCOMPRESSIBLE_DATA = "a".getBytes();
    private static final byte[] COMPRESSIBLE_MULTIBLOCK_DATA = new byte[512 * 1024];
    private static final byte[] INCOMPRESSIBLE_MULTIBLOCK_DATA = new byte[512 * 1024];
    private static final byte[] MIX_MULTIBLOCK_DATA = new byte[512 * 1024];
    private static final LZ4FrameDescriptor CHECKSUM_FRAME_DESCRIPTOR = new LZ4FrameDescriptor(true, 5);

    static {
        Arrays.fill(COMPRESSIBLE_MULTIBLOCK_DATA, (byte) 0xFF);
        new Random(0).nextBytes(INCOMPRESSIBLE_MULTIBLOCK_DATA);
        System.arraycopy(COMPRESSIBLE_MULTIBLOCK_DATA, 0, MIX_MULTIBLOCK_DATA, 0, 256 * 1024);
        System.arraycopy(INCOMPRESSIBLE_MULTIBLOCK_DATA, 0, MIX_MULTIBLOCK_DATA, 256 * 1024, 256 * 1024);
    }

    private InputStream uncompressedStream;
    private LZ4CompressingInputStream compressedStream;

    @After
    public void after() throws IOException {
        uncompressedStream.close();
        compressedStream.close();
    }

    @Test
    public void testReadZeroBytes() throws IOException {
        uncompressedStream = new ByteArrayInputStream(new byte[0]);
        compressedStream = new LZ4CompressingInputStream(uncompressedStream);

        byte[] data = new byte[100];
        int bytesRead = compressedStream.read(data, 0, 0);

        assertEquals(0, bytesRead);
    }

    @Test
    public void testFrameHeaderFormat_defaultFrameDescriptor() throws IOException {
        uncompressedStream = new ByteArrayInputStream(new byte[0]);
        compressedStream = new LZ4CompressingInputStream(uncompressedStream);
        byte[] frameHeader = readFrameHeader();
        int magicValue = LZ4Streams.intFromLittleEndianBytes(frameHeader, 0);
        LZ4FrameDescriptor frameDescriptor = LZ4FrameDescriptor.fromByteArray(
                Arrays.copyOfRange(frameHeader, LZ4Streams.MAGIC_LENGTH, LZ4Streams.FRAME_HEADER_LENGTH));

        assertEquals(LZ4Streams.MAGIC_VALUE, magicValue);
        assertEquals(LZ4Streams.DEFAULT_FRAME_DESCRIPTOR, frameDescriptor);
    }

    @Test
    public void testFrameHeaderFormat_customFrameDescriptor() throws IOException {
        uncompressedStream = new ByteArrayInputStream(new byte[0]);
        compressedStream = new LZ4CompressingInputStream(uncompressedStream, CHECKSUM_FRAME_DESCRIPTOR);
        byte[] frameHeader = readFrameHeader();
        int magicValue = LZ4Streams.intFromLittleEndianBytes(frameHeader, 0);
        LZ4FrameDescriptor frameDescriptor = LZ4FrameDescriptor.fromByteArray(
                Arrays.copyOfRange(frameHeader, LZ4Streams.MAGIC_LENGTH, LZ4Streams.FRAME_HEADER_LENGTH));

        assertEquals(LZ4Streams.MAGIC_VALUE, magicValue);
        assertEquals(CHECKSUM_FRAME_DESCRIPTOR, frameDescriptor);
    }

    @Test
    public void testEmptyStream() throws IOException {
        uncompressedStream = new ByteArrayInputStream(new byte[0]);
        compressedStream = new LZ4CompressingInputStream(uncompressedStream);
        byte[] compressedContents = new byte[100];
        int streamSize = compressedStream.read(compressedContents);
        int blockSize = LZ4Streams.intFromLittleEndianBytes(compressedContents, streamSize - 4);

        assertEquals(LZ4Streams.FRAME_HEADER_LENGTH + LZ4Streams.BLOCK_HEADER_LENGTH, streamSize);
        assertEquals(0, blockSize);
    }

    @Test
    public void testEmptyStream_withContentChecksum() throws IOException {
        uncompressedStream = new ByteArrayInputStream(new byte[0]);
        compressedStream = new LZ4CompressingInputStream(uncompressedStream, CHECKSUM_FRAME_DESCRIPTOR);
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
    public void testSingleBlock_compressed_singleByteRead() throws IOException {
        uncompressedStream = new ByteArrayInputStream(COMPRESSIBLE_DATA);
        compressedStream = new LZ4CompressingInputStream(uncompressedStream);
        readFrameHeader();

        readNextBlockSize();
        int value = compressedStream.read();

        byte[] expectedCompressedData = LZ4Streams.compressor.compress(COMPRESSIBLE_DATA);

        assertEquals(expectedCompressedData[0], value);
    }

    @Test
    public void testSingleBlock_uncompressed_singleByteRead() throws IOException {
        uncompressedStream = new ByteArrayInputStream(INCOMPRESSIBLE_DATA);
        compressedStream = new LZ4CompressingInputStream(uncompressedStream);
        readFrameHeader();

        readNextBlockSize();
        int value = compressedStream.read();

        assertEquals(INCOMPRESSIBLE_DATA[0], value);
    }

    @Test
    public void testSingleBlock_compressed() throws IOException {
        uncompressedStream = new ByteArrayInputStream(COMPRESSIBLE_DATA);
        compressedStream = new LZ4CompressingInputStream(uncompressedStream);
        readFrameHeader();

        int blockSize = readNextBlockSize();
        assertTrue(blockSize > 0);
        byte[] blockData = readNextBlock(blockSize);
        int finalBlockSize = readNextBlockSize();

        byte[] expectedBlockData = LZ4Streams.compressor.compress(COMPRESSIBLE_DATA);

        assertArrayEquals(expectedBlockData, blockData);
        assertEquals(0, finalBlockSize);
        assertStreamIsEmpty();
    }

    @Test
    public void testSingleBlock_uncompressed() throws IOException {
        uncompressedStream = new ByteArrayInputStream(INCOMPRESSIBLE_DATA);
        compressedStream = new LZ4CompressingInputStream(uncompressedStream);
        readFrameHeader();

        int blockSize = readNextBlockSize();
        assertTrue(blockSize < 0);
        byte[] blockData = readNextBlock(blockSize);
        int finalBlockSize = readNextBlockSize();

        assertArrayEquals(INCOMPRESSIBLE_DATA, blockData);
        assertEquals(0, finalBlockSize);
        assertStreamIsEmpty();
    }

    @Test
    public void testMultiBlock_compressed() throws IOException {
        uncompressedStream = new ByteArrayInputStream(COMPRESSIBLE_MULTIBLOCK_DATA);
        compressedStream = new LZ4CompressingInputStream(uncompressedStream);
        readFrameHeader();

        int compressionBlockSize = LZ4Streams.DEFAULT_FRAME_DESCRIPTOR.getMaximumBlockSize();
        int numBlocks = COMPRESSIBLE_MULTIBLOCK_DATA.length / compressionBlockSize;
        // Input data repeats, so each compressed block is the same
        byte[] expectedCompressedBlock =
                LZ4Streams.compressor.compress(Arrays.copyOf(COMPRESSIBLE_MULTIBLOCK_DATA, compressionBlockSize));
        int expectedBlockSize = expectedCompressedBlock.length;

        for (int i = 0; i < numBlocks; ++i) {
            int blockSize = readNextBlockSize();
            assertEquals(expectedBlockSize, blockSize);
            byte[] compressedBlock = readNextBlock(blockSize);
            assertArrayEquals(expectedCompressedBlock, compressedBlock);
        }

        int finalBlockSize = readNextBlockSize();
        assertEquals(0, finalBlockSize);
        assertStreamIsEmpty();
    }

    @Test
    public void testMultiBlock_uncompressed() throws IOException {
        uncompressedStream = new ByteArrayInputStream(INCOMPRESSIBLE_MULTIBLOCK_DATA);
        compressedStream = new LZ4CompressingInputStream(uncompressedStream);
        readFrameHeader();

        int compressionBlockSize = LZ4Streams.DEFAULT_FRAME_DESCRIPTOR.getMaximumBlockSize();
        int numBlocks = INCOMPRESSIBLE_MULTIBLOCK_DATA.length / compressionBlockSize;

        // Uncompressed blocks
        for (int i = 0; i < numBlocks; ++i) {
            int blockSize = readNextBlockSize();
            assertTrue(blockSize < 0);
            int realBlockSize = blockSize ^ 0x80000000;
            assertEquals(compressionBlockSize, realBlockSize);
            byte[] uncompressedBlock = readNextBlock(blockSize);
            // Compare this block with the corresponding block on uncompressed data
            byte[] inputBlock = Arrays.copyOfRange(INCOMPRESSIBLE_MULTIBLOCK_DATA,
                    i * compressionBlockSize, (i + 1) * compressionBlockSize);
            assertArrayEquals(inputBlock, uncompressedBlock);
        }

        int finalBlockSize = readNextBlockSize();
        assertEquals(0, finalBlockSize);
        assertStreamIsEmpty();
    }

    @Test
    public void testMultiBlock_mixed() throws IOException {
        uncompressedStream = new ByteArrayInputStream(MIX_MULTIBLOCK_DATA);
        compressedStream = new LZ4CompressingInputStream(uncompressedStream);
        readFrameHeader();

        int compressionBlockSize = LZ4Streams.DEFAULT_FRAME_DESCRIPTOR.getMaximumBlockSize();
        int numBlocks = MIX_MULTIBLOCK_DATA.length / compressionBlockSize;
        // Compressed block inputs repeat, so each compressed block is the same
        byte[] expectedCompressedBlock =
                LZ4Streams.compressor.compress(Arrays.copyOf(COMPRESSIBLE_MULTIBLOCK_DATA, compressionBlockSize));
        int expectedCompressedBlockSize = expectedCompressedBlock.length;

        // Compressed blocks
        for (int i = 0; i < numBlocks / 2; ++i) {
            int blockSize = readNextBlockSize();
            assertEquals(expectedCompressedBlockSize, blockSize);
            byte[] compressedBlock = readNextBlock(blockSize);
            assertArrayEquals(expectedCompressedBlock, compressedBlock);
        }

        // Uncompressed blocks
        for (int i = 0; i < numBlocks / 2; ++i) {
            int blockSize = readNextBlockSize();
            assertTrue(blockSize < 0);
            int realBlockSize = blockSize ^ 0x80000000;
            assertEquals(compressionBlockSize, realBlockSize);
            byte[] uncompressedBlock = readNextBlock(blockSize);
            // Compare this block with the corresponding block on uncompressed data
            byte[] inputBlock = Arrays.copyOfRange(INCOMPRESSIBLE_MULTIBLOCK_DATA,
                    i * compressionBlockSize, (i + 1) * compressionBlockSize);
            assertArrayEquals(inputBlock, uncompressedBlock);
        }

        int finalBlockSize = readNextBlockSize();
        assertEquals(0, finalBlockSize);
        assertStreamIsEmpty();
    }

    @Test
    public void testMultiBlock_mixed_contentChecksum() throws IOException {
        uncompressedStream = new ByteArrayInputStream(MIX_MULTIBLOCK_DATA);
        compressedStream = new LZ4CompressingInputStream(uncompressedStream, CHECKSUM_FRAME_DESCRIPTOR);
        readFrameHeader();
        skipDataBlocks();

        int compressionBlockSize = CHECKSUM_FRAME_DESCRIPTOR.getMaximumBlockSize();
        StreamingXXHash32 hash = XXHashFactory.fastestInstance().newStreamingHash32(0);
        int inputLocation = 0;
        while (inputLocation < MIX_MULTIBLOCK_DATA.length) {
            hash.update(MIX_MULTIBLOCK_DATA, inputLocation, compressionBlockSize);
            inputLocation += compressionBlockSize;
        }
        int expectedHash = hash.getValue();

        byte[] hashBytes = new byte[4];
        int bytesRead = compressedStream.read(hashBytes);
        assertEquals(4, bytesRead);
        int hashValue = LZ4Streams.intFromLittleEndianBytes(hashBytes, 0);

        assertEquals(expectedHash, hashValue);
        assertStreamIsEmpty();
    }

    @Test
    public void testMultiBlock_readMultipleBlocksAtOnce() throws IOException {
        uncompressedStream = new ByteArrayInputStream(COMPRESSIBLE_MULTIBLOCK_DATA);
        compressedStream = new LZ4CompressingInputStream(uncompressedStream);

        int compressionBlockSize = LZ4Streams.DEFAULT_FRAME_DESCRIPTOR.getMaximumBlockSize();
        int numBlocks = COMPRESSIBLE_MULTIBLOCK_DATA.length / compressionBlockSize;
        // Input data repeats, so each compressed block is the same
        byte[] expectedCompressedBlock =
                LZ4Streams.compressor.compress(Arrays.copyOf(COMPRESSIBLE_MULTIBLOCK_DATA, compressionBlockSize));
        int expectedBlockSize = expectedCompressedBlock.length;

        int expectedStreamSize = LZ4Streams.FRAME_HEADER_LENGTH
                + numBlocks * expectedBlockSize
                + (numBlocks + 1) * LZ4Streams.BLOCK_HEADER_LENGTH;
        byte[] data = new byte[expectedStreamSize];
        int bytesRead = compressedStream.read(data);

        assertEquals(expectedStreamSize, bytesRead);
        assertStreamIsEmpty();
    }

    private byte[] readFrameHeader() throws IOException {
        byte[] header = new byte[LZ4Streams.FRAME_HEADER_LENGTH];
        int bytesRead = compressedStream.read(header);
        assertEquals(LZ4Streams.FRAME_HEADER_LENGTH, bytesRead);
        return header;
    }

    // For each block, reads the block size and then skips the block data.
    // Terminates after a zero size block size is read.
    private void skipDataBlocks() throws IOException {
        int blockSize = readNextBlockSize();
        while (blockSize != 0) {
            readNextBlock(blockSize);
            blockSize = readNextBlockSize();
        }
    }

    private int readNextBlockSize() throws IOException {
        byte[] blockSizeBytes = new byte[4];
        int bytesRead = compressedStream.read(blockSizeBytes, 0, 4);
        assertEquals(4, bytesRead);
        return LZ4Streams.intFromLittleEndianBytes(blockSizeBytes, 0);
    }

    private byte[] readNextBlock(int blockSize) throws IOException {
        if (blockSize < 0) {
            // Uncompressed block. Toggle the highest order bit to get the true block size.
            blockSize ^= 0x80000000;
        }
        byte[] data = new byte[blockSize];
        int bytesRead = compressedStream.read(data, 0, blockSize);
        assertEquals(blockSize, bytesRead);

        return data;
    }

    private void assertStreamIsEmpty() throws IOException {
        assertEquals(-1, compressedStream.read());
    }

}
