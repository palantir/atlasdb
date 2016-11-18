/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.common.compression;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

public class LZ4CompressionTests {

    private static final String COMPRESSIBLE_MESSAGE = "aaaaaaaaaaaaaaa";
    private static final String INCOMPRESSIBLE_MESSAGE = "a";

    @Test
    public void testFrameHeaderFormat_defaultFrameDescriptor() throws IOException {
        InputStream uncompressedStream = new ByteArrayInputStream(COMPRESSIBLE_MESSAGE.getBytes());
        LZ4CompressingInputStream compressedStream = new LZ4CompressingInputStream(uncompressedStream);
        byte[] frameHeader = new byte[LZ4Streams.FRAME_HEADER_LENGTH];
        compressedStream.read(frameHeader, 0, LZ4Streams.FRAME_HEADER_LENGTH);
        compressedStream.close();

        int magicValue = LZ4DecompressingInputStream.readIntLittleEndian(frameHeader, 0);
        Assert.assertEquals(magicValue, LZ4Streams.MAGIC_VALUE);
        LZ4FrameDescriptor frameDescriptor = LZ4FrameDescriptor.fromByteArray(
                Arrays.copyOfRange(frameHeader, LZ4Streams.MAGIC_LENGTH, LZ4Streams.FRAME_HEADER_LENGTH));
        Assert.assertEquals(frameDescriptor, LZ4Streams.DEFAULT_FRAME_DESCRIPTOR);
    }

    @Test
    public void testFrameHeaderFormat_nondefaultFrameDescriptor() throws IOException {
        LZ4FrameDescriptor expectedFrameDescriptor = new LZ4FrameDescriptor(true, 7);
        InputStream uncompressedStream = new ByteArrayInputStream(COMPRESSIBLE_MESSAGE.getBytes());
        LZ4CompressingInputStream compressedStream =
                new LZ4CompressingInputStream(uncompressedStream, expectedFrameDescriptor);
        byte[] frameHeader = new byte[LZ4Streams.FRAME_HEADER_LENGTH];
        compressedStream.read(frameHeader, 0, LZ4Streams.FRAME_HEADER_LENGTH);
        compressedStream.close();

        int magicValue = LZ4DecompressingInputStream.readIntLittleEndian(frameHeader, 0);
        Assert.assertEquals(magicValue, LZ4Streams.MAGIC_VALUE);
        LZ4FrameDescriptor frameDescriptor = LZ4FrameDescriptor.fromByteArray(
                Arrays.copyOfRange(frameHeader, LZ4Streams.MAGIC_LENGTH, LZ4Streams.FRAME_HEADER_LENGTH));
        Assert.assertEquals(frameDescriptor, expectedFrameDescriptor);
    }

    @Test
    public void testFrameFooterFormat_noHash() throws IOException {
        InputStream uncompressedStream = new ByteArrayInputStream(COMPRESSIBLE_MESSAGE.getBytes());
        LZ4CompressingInputStream compressedStream = new LZ4CompressingInputStream(uncompressedStream);
        byte[] buffer = new byte[1000];
        compressedStream.read(buffer, 0, LZ4Streams.FRAME_HEADER_LENGTH);
        compressedStream.read(buffer, 0, 4);
        int blockSize = LZ4DecompressingInputStream.readIntLittleEndian(buffer, 0);
        if (blockSize < 0) {
            blockSize ^= 0x80000000;
        }
        compressedStream.read(buffer, 0, blockSize);
        int footerSize = compressedStream.read(buffer);
        compressedStream.close();

        Assert.assertEquals(footerSize, 4);
        for (int i=0; i < 4; ++i) {
            Assert.assertEquals(0, buffer[i]);
        }
    }

    @Test
    public void testFrameFooterFormat_withHash() throws IOException {
        InputStream uncompressedStream = new ByteArrayInputStream(COMPRESSIBLE_MESSAGE.getBytes());
        LZ4CompressingInputStream compressedStream = new LZ4CompressingInputStream(uncompressedStream,
                new LZ4FrameDescriptor(true, 4));

        StreamingXXHash32 hasher = XXHashFactory.fastestInstance().newStreamingHash32(0);
        hasher.update(COMPRESSIBLE_MESSAGE.getBytes(), 0, COMPRESSIBLE_MESSAGE.getBytes().length);
        int expectedHash = hasher.getValue();

        byte[] buffer = new byte[1000];
        compressedStream.read(buffer, 0, LZ4Streams.FRAME_HEADER_LENGTH);
        compressedStream.read(buffer, 0, LZ4Streams.HEADER_SIZE);
        int blockSize = LZ4DecompressingInputStream.readIntLittleEndian(buffer, 0);
        compressedStream.read(buffer, 0, blockSize);

        // Read footer
        int footerSize = compressedStream.read(buffer);
        compressedStream.close();
        Assert.assertEquals(8, footerSize);
        for (int i = 0; i < 4; ++i) {
            Assert.assertEquals(0, buffer[i]);
        }
        int hash = LZ4DecompressingInputStream.readIntLittleEndian(buffer, 4);
        Assert.assertEquals(expectedHash, hash);
    }

    @Test
    public void testHashDecompression() throws IOException {
        InputStream uncompressedStream = new ByteArrayInputStream(COMPRESSIBLE_MESSAGE.getBytes());
        LZ4CompressingInputStream compressedStream = new LZ4CompressingInputStream(uncompressedStream,
                new LZ4FrameDescriptor(true, 4));
        LZ4DecompressingInputStream decompressedStream = new LZ4DecompressingInputStream(compressedStream);
        byte[] buffer = new byte[1000];
        decompressedStream.read(buffer);
        decompressedStream.close();
    }

    @Test
    public void testIncompressibleData() throws IOException {
        InputStream inputStream = new ByteArrayInputStream(INCOMPRESSIBLE_MESSAGE.getBytes());
        LZ4CompressingInputStream compressedStream = new LZ4CompressingInputStream(inputStream);
        int actualSize = INCOMPRESSIBLE_MESSAGE.getBytes().length;

        //Inspect the single block manually
        byte[] buffer = new byte[1000];
        compressedStream.read(buffer, 0, 11);
        int blockSize = LZ4DecompressingInputStream.readIntLittleEndian(buffer, 7);
        Assert.assertTrue(blockSize < 0);
        Assert.assertEquals(actualSize, blockSize ^ 0x80000000);
        blockSize ^= 0x80000000;
        compressedStream.read(buffer, 0, blockSize);
        compressedStream.close();
        Assert.assertArrayEquals(INCOMPRESSIBLE_MESSAGE.getBytes(), Arrays.copyOf(buffer, blockSize));

        //Use the decompressing stream
        inputStream = new ByteArrayInputStream(INCOMPRESSIBLE_MESSAGE.getBytes());
        compressedStream = new LZ4CompressingInputStream(inputStream);
        LZ4DecompressingInputStream decompressedStream = new LZ4DecompressingInputStream(compressedStream);
        int decompressedSize = decompressedStream.read(buffer);
        decompressedStream.close();
        Assert.assertEquals(actualSize, decompressedSize);
        Assert.assertArrayEquals(INCOMPRESSIBLE_MESSAGE.getBytes(), Arrays.copyOfRange(buffer, 0, decompressedSize));
    }
}
