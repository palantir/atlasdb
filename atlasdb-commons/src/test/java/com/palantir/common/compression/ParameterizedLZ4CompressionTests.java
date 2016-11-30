/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.common.compression;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ParameterizedLZ4CompressionTests {

    private static LZ4FrameDescriptor HASHING_FRAME_DESCRIPTOR = new LZ4FrameDescriptor(true, 4);

    private String filePath;
    private FileInputStream fileStream;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {"src/test/resources/compressionTestFile_128KB_compressible"},
            {"src/test/resources/compressionTestFile_128KB_incompressible"}
        });
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public ParameterizedLZ4CompressionTests(String filePath) throws FileNotFoundException {
        this.filePath = filePath;
        fileStream = new FileInputStream(filePath);
    }

    @Test
    public void testCompressAndDecompress() throws IOException {
        LZ4CompressingInputStream compressedStream = new LZ4CompressingInputStream(fileStream);
        LZ4DecompressingInputStream decompressedStream = new LZ4DecompressingInputStream(compressedStream);
        byte[] decompressedData = new byte[1000000];
        int decompressedSize = decompressedStream.read(decompressedData);
        decompressedStream.close();
        byte[] fileContents = getFileContents();

        Assert.assertArrayEquals(fileContents, Arrays.copyOf(decompressedData, decompressedSize));
    }

    @Test
    public void testCompressAndDecompress_withHashing() throws IOException {
        LZ4CompressingInputStream compressedStream = new LZ4CompressingInputStream(fileStream, HASHING_FRAME_DESCRIPTOR);
        LZ4DecompressingInputStream decompressedStream = new LZ4DecompressingInputStream(compressedStream);
        byte[] decompressedData = new byte[1000000];
        int decompressedSize = decompressedStream.read(decompressedData);
        decompressedStream.close();
        byte[] fileContents = getFileContents();

        // Decompressing stream validates the hash internally
        Assert.assertArrayEquals(fileContents, Arrays.copyOf(decompressedData, decompressedSize));
    }

    @Test
    public void testCompressAndDecompress_hashVerificationFailure() throws IOException {
        LZ4CompressingInputStream compressedStream = new LZ4CompressingInputStream(fileStream, HASHING_FRAME_DESCRIPTOR);
        byte[] buffer = new byte[1000000];
        int size = compressedStream.read(buffer);
        compressedStream.close();

        //Modify the hash value
        int originalHash = LZ4Streams.littleEndianIntFromBytes(buffer, size - 4);
        for (int i = size - 4; i < size; ++i) {
            buffer[i] = 0x00;
        }
        InputStream modifiedStream = new ByteArrayInputStream(buffer);
        LZ4DecompressingInputStream decompressedStream = new LZ4DecompressingInputStream(modifiedStream);

        String expectedMessage = String.format("LZ4 content did not match checksum: content sums to %s, "
                + "but checksum was 0", originalHash);
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(expectedMessage);
        byte[] decompressedBytes = new byte[1000000];
        decompressedStream.read(decompressedBytes);
        decompressedStream.close();
    }

    private byte[] getFileContents() throws IOException {
        FileInputStream fileStream = new FileInputStream(filePath);
        byte[] fileContents = new byte[1000000];
        int fileSize = fileStream.read(fileContents);
        fileStream.close();
        return Arrays.copyOf(fileContents, fileSize);
    }

}
