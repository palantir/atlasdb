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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import com.google.common.base.Preconditions;

import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * {@link InputStream} that wraps an InputStream that is compressed
 * according to the v1.5.0 LZ4 specification. Buffers and decompresses
 * the content of the delegate stream.
 */
public class LZ4DecompressingInputStream extends BufferedDelegateInputStream {

    private final LZ4SafeDecompressor decompressor;
    private final LZ4FrameDescriptor frameDescriptor;
    private final byte[] compressedBuffer;
    private final StreamingXXHash32 hasher;

    // Length in bytes of the next compressed block to be read from the delegate
    private int nextLength;

    public LZ4DecompressingInputStream(InputStream delegate) throws IOException {
        super(delegate);
        this.decompressor = LZ4Streams.decompressor;
        this.hasher = XXHashFactory.fastestInstance().newStreamingHash32(0);
        this.frameDescriptor = readFrameHeader();
        this.compressedBuffer = new byte[LZ4Streams.getCompressedBufferSize(frameDescriptor)];

        allocateBuffer(LZ4Streams.getUncompressedBufferSize(frameDescriptor));
    }

    // Reads the frame header as well as the size of the first block
    private LZ4FrameDescriptor readFrameHeader() throws IOException {
        byte[] frameHeader =
                new byte[LZ4Streams.MAGIC_LENGTH + LZ4Streams.FRAME_DESCRIPTOR_LENGTH
                         + LZ4Streams.BLOCK_HEADER_LENGTH];
        nextLength = readBlock(frameHeader, frameHeader.length);
        Preconditions.checkState(LZ4Streams.intFromLittleEndianBytes(frameHeader, 0) == LZ4Streams.MAGIC_VALUE,
                "Input is not an lz4 stream that this input stream can decompress");
        return LZ4FrameDescriptor.fromByteArray(Arrays.copyOfRange(frameHeader,
                LZ4Streams.MAGIC_LENGTH, LZ4Streams.MAGIC_LENGTH + LZ4Streams.FRAME_DESCRIPTOR_LENGTH));
    }

    // This reads one block plus the size of the next block in as few
    // read calls as possible. The length should be the number of bytes
    // of data plus HEADER_SIZE bytes for the length of the next block.
    private int readBlock(byte[] b, int length) throws IOException {
        int bytesReadSoFar = 0;
        while (bytesReadSoFar < length) {
            int numRead = delegate.read(b, bytesReadSoFar, length - bytesReadSoFar);
            if (numRead < 0) {
                throw new EOFException("Stream ended prematurely");
            }
            bytesReadSoFar += numRead;
        }

        return LZ4Streams.intFromLittleEndianBytes(b, length - 4);
    }

    // Refills the internal buffer by decompressing, if applicable, the
    // next block of data from the delegate input stream.
    @Override
    protected boolean refill() throws IOException {
        int length = nextLength;

        if (length == 0) {
            // The next block has size zero, so there's no remaining bytes
            // to read and this stream cannot be refilled. If the content
            // checksum is enabled, validate the streamed data against the
            // checksum in the LZ4 footer.
            if (frameDescriptor.hasContentChecksum) {
                verifyContentChecksum();
            }
            return false;
        }

        if (length > 0) {
            // A positive length indicates a compressed block, so read
            // the block and decompress it.
            nextLength = readBlock(compressedBuffer, length + LZ4Streams.BLOCK_HEADER_LENGTH);
            maxPosition = decompressor.decompress(compressedBuffer, BUFFER_START, length, buffer, BUFFER_START,
                    frameDescriptor.maximumBlockSize);
        } else {
            // A negative length indicates that the upcoming block is
            // uncompressed. The true size is the length with the highest
            // order bit toggled.
            length ^= 0x80000000;
            nextLength = readBlock(buffer, length + LZ4Streams.BLOCK_HEADER_LENGTH);
            maxPosition = length;
        }

        if (frameDescriptor.hasContentChecksum) {
            // Update the running hash with the decompressed data
            hasher.update(buffer, 0, maxPosition);
        }

        position = 0;
        return true;
    }

    // Reads the content checksum from the LZ4 footer and compares it with
    // the value obtained from the decompressed data read through the stream.
    private void verifyContentChecksum() throws IOException {
        byte[] bytes = new byte[4];
        if (delegate.read(bytes) < 4) {
            throw new EOFException("Could not verify stream checksum because stream ended prematurely");
        }
        int computedChecksum = hasher.getValue();
        int providedChecksum = LZ4Streams.intFromLittleEndianBytes(bytes, 0);
        Preconditions.checkState(computedChecksum == providedChecksum,
                "LZ4 content did not match checksum: content sums to %s, but checksum was %s",
                computedChecksum, providedChecksum);
    }

}
