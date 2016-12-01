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

import java.io.IOException;
import java.io.InputStream;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * {@link InputStream} that buffers a delegate InputStream, compressing
 * the stream as it is read. The compressed stream format corresponds to the
 * v1.5.0 LZ4 specification.
 *
 * The resulting stream can be decompressed with the {@link LZ4DecompressingInputStream}.
 */
public final class LZ4CompressingInputStream extends BufferedDelegateInputStream {

    private final LZ4Compressor compressor;
    private final LZ4FrameDescriptor frameDescriptor;
    private final byte[] uncompressedBuffer;
    private final StreamingXXHash32 hasher;

    // True if the delegate stream has been exhausted and the LZ4
    // footer has been written to the compressed buffer.
    private boolean inFrameFooter;

    public LZ4CompressingInputStream(InputStream delegate) throws IOException {
        this(delegate, LZ4Streams.DEFAULT_FRAME_DESCRIPTOR);
    }

    public LZ4CompressingInputStream(InputStream delegate, LZ4FrameDescriptor frameDescriptor) throws IOException {
        super(delegate, LZ4Streams.getCompressedBufferSize(frameDescriptor));
        this.compressor = LZ4Streams.compressor;
        this.frameDescriptor = frameDescriptor;
        this.uncompressedBuffer = new byte[LZ4Streams.getUncompressedBufferSize(frameDescriptor)];
        this.hasher = XXHashFactory.fastestInstance().newStreamingHash32(0);
        this.inFrameFooter = false;

        writeFrameHeader();
    }

    private void writeFrameHeader() {
        writeMagicValue();
        writeFrameDescriptor();
        maxPosition = LZ4Streams.FRAME_HEADER_LENGTH;
    }

    private void writeMagicValue() {
        byte[] magicValue = LZ4Streams.littleEndianIntToBytes(LZ4Streams.MAGIC_VALUE);
        copyToStartOfBuffer(magicValue, 0, 4);
    }

    private void writeFrameDescriptor() {
        byte[] frameDescriptorBytes = frameDescriptor.toByteArray();
        System.arraycopy(frameDescriptorBytes, 0, buffer, LZ4Streams.MAGIC_LENGTH, LZ4Streams.FRAME_DESCRIPTOR_LENGTH);
    }

    private void copyToStartOfBuffer(byte[] src, int off, int len) {
        System.arraycopy(src, off, buffer, BUFFER_START, len);
    }

    // Refills the internal buffer by compressing a block of data from
    // the delegate input stream. If the block cannot be compressed, the
    // uncompressed bytes are written instead.
    @Override
    protected boolean refill() throws IOException {
        if (inFrameFooter) {
            // The delegate stream is exhausted and the frame footer has
            // already been read, so the buffer can't be refilled.
            return false;
        }

        int numRead = delegate.read(uncompressedBuffer, 0, frameDescriptor.maximumBlockSize);
        if (numRead == READ_FAILED) {
            // The delegate stream is exhausted, so we write the LZ4 frame footer.
            inFrameFooter = true;
            writeFooter();
        } else {
            if (frameDescriptor.hasContentChecksum) {
                // Update the running hash with the uncompressed data
                hasher.update(uncompressedBuffer, 0, numRead);
            }
            compressBuffer(numRead);
        }

        return true;
    }

    private void writeFooter() throws IOException {
        // Write 0 for the size of the next block to indicate no blocks are left
        writeBlockSize(0);
        position = 0;
        maxPosition = LZ4Streams.BLOCK_HEADER_LENGTH;
        if (frameDescriptor.hasContentChecksum) {
            // Write the final content hash
            byte[] hashValue = LZ4Streams.littleEndianIntToBytes(hasher.getValue());
            System.arraycopy(hashValue, 0, buffer, LZ4Streams.BLOCK_HEADER_LENGTH, 4);
            maxPosition = LZ4Streams.BLOCK_HEADER_LENGTH + 4;
        }
    }

    private void writeBlockSize(int length) throws IOException {
        byte[] blockSize = LZ4Streams.littleEndianIntToBytes(length);
        copyToStartOfBuffer(blockSize, 0, 4);
    }


    // If the data in the uncompressed buffer is compressible, writes
    // the compressed data to the buffer. Otherwise, writes the
    // uncompressed data instead. The data is prepended by the size of
    // the block.
    private void compressBuffer(int uncompressedBufferSize) throws IOException {
        // Leave space at the beginning of the buffer for the block size.
        int compressedBufferMaxSize = buffer.length - LZ4Streams.BLOCK_HEADER_LENGTH;
        int compressedLength = compressor.compress(uncompressedBuffer, 0, uncompressedBufferSize,
                buffer, LZ4Streams.BLOCK_HEADER_LENGTH, compressedBufferMaxSize);

        if (compressedLength >= uncompressedBufferSize) {
            // The compressed data is longer than the original data, so
            // we write the uncompressed data instead. For uncompressed
            // blocks, the highest bit in the block size should be 1 so
            // that the data is not decompressed when read.
            System.arraycopy(uncompressedBuffer, 0, buffer, LZ4Streams.BLOCK_HEADER_LENGTH, uncompressedBufferSize);
            maxPosition = LZ4Streams.BLOCK_HEADER_LENGTH + uncompressedBufferSize;
            // The maximum LZ4 supported buffer size is 4 MB, so the highest
            // order bit is unused and is 0. It's toggled to 1 to indicate an
            // uncompressed block.
            int modifiedBlockSize = uncompressedBufferSize ^ 0x80000000;
            writeBlockSize(modifiedBlockSize);
        } else {
            maxPosition = LZ4Streams.BLOCK_HEADER_LENGTH + compressedLength;
            writeBlockSize(compressedLength);
        }

        // Reset buffer position to the front of the newly refilled buffer.
        position = 0;
    }
}
