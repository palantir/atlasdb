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
package com.palantir.atlasdb.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.schema.stream.StreamStoreDefinition;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public final class BlockConsumingInputStream extends InputStream {
    private final BlockGetter blockGetter;
    private final long numBlocks;
    private final int blocksInMemory;

    private long nextBlockToRead;

    private byte[] buffer;
    private int positionInBuffer;

    public static BlockConsumingInputStream create(BlockGetter blockGetter, long numBlocks, int blocksInMemory)
            throws IOException {
        ensureExpectedArraySizeDoesNotOverflow(blockGetter, blocksInMemory);
        return new BlockConsumingInputStream(blockGetter, numBlocks, blocksInMemory);
    }

    // we don't want to actually create a very large array in tests, as the external test VM would run out of memory.
    @VisibleForTesting
    static void ensureExpectedArraySizeDoesNotOverflow(BlockGetter blockGetter, int blocksInMemory) {
        int expectedBlockLength = blockGetter.expectedBlockLength();
        int maxBlocksInMemory = StreamStoreDefinition.MAX_IN_MEMORY_THRESHOLD / expectedBlockLength;
        long expectedBufferSize = (long) expectedBlockLength * (long) blocksInMemory;
        Preconditions.checkArgument(
                blocksInMemory <= maxBlocksInMemory,
                "Promised to load too many blocks into memory. The underlying buffer is stored as a byte array, "
                        + "so can only fit %s bytes. The supplied BlockGetter expected to produce "
                        + "blocks of %s bytes, so %s of them (requested size %s) would cause the array to overflow.",
                StreamStoreDefinition.MAX_IN_MEMORY_THRESHOLD,
                expectedBlockLength,
                blocksInMemory,
                expectedBufferSize);
    }

    private BlockConsumingInputStream(BlockGetter blockGetter, long numBlocks, int blocksInMemory) {
        this.blockGetter = blockGetter;
        this.numBlocks = numBlocks;
        this.blocksInMemory = blocksInMemory;
        this.nextBlockToRead = 0L;
        this.positionInBuffer = 0;
        this.buffer = new byte[0];
    }

    @Override
    public int read() throws IOException {
        if (positionInBuffer < buffer.length) {
            return buffer[positionInBuffer++] & 0xff;
        }

        if (nextBlockToRead < numBlocks) {
            boolean reloaded = refillBuffer();
            if (!reloaded) {
                return -1;
            }

            return buffer[positionInBuffer++] & 0xff;
        }

        return -1;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        com.palantir.logsafe.Preconditions.checkNotNull(bytes, "Cannot read into a null array!");
        if (off < 0 || len < 0 || len > bytes.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return 0;
        }

        int bytesRead = 0;
        while (bytesRead < len) {
            int bytesLeftInBuffer = buffer.length - positionInBuffer;
            int bytesToCopy = Math.min(bytesLeftInBuffer, len - bytesRead);
            System.arraycopy(buffer, positionInBuffer, bytes, off + bytesRead, bytesToCopy);
            positionInBuffer += bytesToCopy;
            bytesRead += bytesToCopy;

            if (positionInBuffer >= buffer.length) {
                boolean reloaded = refillBuffer();
                if (!reloaded) {
                    break;
                }
            }
        }

        if (bytesRead == 0) {
            return -1;
        }

        return bytesRead;
    }

    private boolean refillBuffer() throws IOException {
        // since blocksInMemory is an int, the min is guaranteed to fit in an int
        int numBlocksToGet = (int) Math.min(blocksLeft(), blocksInMemory);
        if (numBlocksToGet <= 0) {
            return false;
        }

        int expectedLength = blockGetter.expectedBlockLength() * numBlocksToGet;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(expectedLength)) {
            blockGetter.get(nextBlockToRead, numBlocksToGet, outputStream);
            nextBlockToRead += numBlocksToGet;
            buffer = outputStream.toByteArray();
            positionInBuffer = 0;
            return true;
        }
    }

    private long blocksLeft() {
        return Math.max(0L, numBlocks - nextBlockToRead);
    }
}
