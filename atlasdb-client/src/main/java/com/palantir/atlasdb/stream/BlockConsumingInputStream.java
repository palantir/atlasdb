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
package com.palantir.atlasdb.stream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public final class BlockConsumingInputStream extends InputStream {
    private final BlockGetter blockGetter;
    private final long numBlocks;
    private final int blocksInMemory;

    private int nextBlockToRead;

    private byte[] buffer;
    private int positionInBuffer;

    public static BlockConsumingInputStream create(
            BlockGetter blockGetter,
            long numBlocks,
            int blocksInMemory) throws IOException {
        BlockConsumingInputStream stream = new BlockConsumingInputStream(blockGetter, numBlocks, blocksInMemory);
        stream.getNextBlock();
        return stream;
    }

    private BlockConsumingInputStream(BlockGetter blockGetter, long numBlocks, int blocksInMemory) {
        this.blockGetter = blockGetter;
        this.numBlocks = numBlocks;
        this.blocksInMemory = blocksInMemory;
        this.nextBlockToRead = 0;
        this.positionInBuffer = 0;
    }

    @Override
    public int read() throws IOException {
        if (positionInBuffer < buffer.length) {
            return buffer[positionInBuffer++] & 0xff;
        }

        if (nextBlockToRead < numBlocks) {
            getNextBlock();
            return buffer[positionInBuffer++] & 0xff;
        }

        return -1;
    }

    private void getNextBlock() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int numBlocksToGet = Math.min(blocksLeft(), blocksInMemory);
        blockGetter.get(nextBlockToRead, numBlocksToGet, outputStream);
        nextBlockToRead += numBlocksToGet;
        buffer = outputStream.toByteArray();
        positionInBuffer = 0;
        outputStream.close();
    }

    private int blocksLeft() {
        return (int) numBlocks - nextBlockToRead;
    }
}
