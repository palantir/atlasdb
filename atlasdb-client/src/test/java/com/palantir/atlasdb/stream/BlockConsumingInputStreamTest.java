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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.palantir.atlasdb.schema.stream.StreamStoreDefinition;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class BlockConsumingInputStreamTest {
    private static final int DATA_SIZE = 4;
    private static final int DATA_SIZE_PLUS_ONE = 5;

    private final byte[] data = "data".getBytes(StandardCharsets.UTF_8);
    private final BlockGetter dataConsumer = new BlockGetter() {
        @Override
        public void get(long firstBlock, long numBlocks, OutputStream destination) {
            try {
                destination.write(data);
            } catch (IOException e) {
                fail("fail");
            }
        }

        @Override
        public int expectedBlockLength() {
            return data.length;
        }
    };

    private final BlockGetter singleByteConsumer = new BlockGetter() {
        @Override
        public void get(long offset, long numBlocks, OutputStream os) {
            try {
                os.write(data, (int) offset, (int) numBlocks);
            } catch (IOException e) {
                fail("fail");
            }
        }

        @Override
        public int expectedBlockLength() {
            return data.length;
        }
    };

    private final byte[] stored = "divisible".getBytes(StandardCharsets.UTF_8);
    private final BlockGetter threeByteConsumer = new BlockGetter() {
        @Override
        public void get(long offset, long numBlocks, OutputStream os) {
            try {
                os.write(stored, 3 * (int) offset, 3 * (int) numBlocks);
            } catch (IOException e) {
                fail("fail");
            }
        }

        @Override
        public int expectedBlockLength() {
            return data.length;
        }
    };

    private BlockConsumingInputStream dataStream;

    @Before
    public void setUp() throws Exception {
        dataStream = BlockConsumingInputStream.create(dataConsumer, 1, 1);
    }

    @Test(expected = NullPointerException.class)
    public void cantReadToNullArray() throws IOException {
        dataStream.read(null, 1, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void cantReadToNegativePlace() throws IOException {
        dataStream.read(data, -1, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void cantReadNegativeAmount() throws IOException {
        dataStream.read(data, 0, -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void cantReadMoreThanArrayLength() throws IOException {
        dataStream.read(data, 0, 10);
    }

    @Test
    public void canReadSingleByte() throws IOException {
        int byteAsInt = dataStream.read();
        byte[] readByte = {(byte) byteAsInt};
        assertThat(new String(readByte, StandardCharsets.UTF_8)).isEqualTo("d");
    }

    @Test
    public void readEmptyArrayReturnsZero() throws IOException {
        int read = dataStream.read(new byte[0]);
        assertThat(read).isEqualTo(0);
    }

    @Test
    public void canReadBlock() throws IOException {
        byte[] result = new byte[DATA_SIZE];
        int read = dataStream.read(result);
        assertThat(read).isEqualTo(DATA_SIZE);
        assertThat(result).isEqualTo(data);
    }

    @Test
    public void canReadAcrossBlocks() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(threeByteConsumer, 2, 1);
        expectNextBytesFromStream(stream, "di");
        expectNextBytesFromStream(stream, "vi");
        expectNextBytesFromStream(stream, "si");
    }

    @Test
    public void canReadAcrossBlocksWithIncompleteFinalBlock() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(threeByteConsumer, 3, 2);
        expectNextBytesFromStream(stream, "di");
        expectNextBytesFromStream(stream, "vi");
        expectNextBytesFromStream(stream, "si");
        expectNextBytesFromStream(stream, "bl");

        byte[] chunk = new byte[2];
        int read = stream.read(chunk);
        assertThat(read).isEqualTo(1);
        assertThat(Arrays.copyOf(chunk, 1)).isEqualTo("e".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void readSingleByteWhenStreamExhaustedReturnsMinusOne() throws IOException {
        dataStream.read(new byte[DATA_SIZE]);

        int read = dataStream.read();
        assertThat(read).isEqualTo(-1);
    }

    @Test
    public void readWhenStreamExhaustedReturnsMinusOne() throws IOException {
        byte[] result = new byte[DATA_SIZE];
        dataStream.read(result);

        int read = dataStream.read(result);
        assertThat(read).isEqualTo(-1);
    }

    @Test
    public void largerArraysThanDataGetPartiallyFilled() throws IOException {
        byte[] result = new byte[DATA_SIZE_PLUS_ONE];
        int read = dataStream.read(result);
        assertThat(read).isEqualTo(DATA_SIZE);
        assertThat(Arrays.copyOf(result, DATA_SIZE)).isEqualTo(data);
    }

    @Test
    public void canReadMultipleBlocks() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(singleByteConsumer, DATA_SIZE, 1);
        byte[] result = new byte[DATA_SIZE];
        int read = stream.read(result);
        assertThat(read).isEqualTo(DATA_SIZE);
        assertThat(result).isEqualTo(data);
    }

    @Test
    public void canReadMultipleBlocksAndPartiallyFillResult() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(singleByteConsumer, DATA_SIZE, 1);
        byte[] result = new byte[DATA_SIZE_PLUS_ONE];
        int read = stream.read(result);
        assertThat(read).isEqualTo(DATA_SIZE);
        assertThat(Arrays.copyOf(result, DATA_SIZE)).isEqualTo(data);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void passingInTooManyBlocksCausesAnException() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(singleByteConsumer, DATA_SIZE_PLUS_ONE, 1);
        byte[] result = new byte[DATA_SIZE_PLUS_ONE];
        stream.read(result);
    }

    @Test
    public void passingInTooFewBlocksCausesIncompleteOutput() throws IOException {
        int dataSizeMinusOne = 3;
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(singleByteConsumer, dataSizeMinusOne, 1);
        byte[] result = new byte[DATA_SIZE];
        int read = stream.read(result);
        assertThat(read).isEqualTo(dataSizeMinusOne);
        assertThat(Arrays.copyOf(result, dataSizeMinusOne)).isEqualTo(Arrays.copyOf(data, dataSizeMinusOne));
    }

    @Test
    public void bufferLengthCanAlmostReachIntMaxValue() throws IOException {
        BlockGetter bigGetter = new BlockGetter() {
            @Override
            public void get(long firstBlock, long numBlocks, OutputStream destination) {
                // do nothing
            }

            @Override
            public int expectedBlockLength() {
                return Integer.MAX_VALUE - 8;
            }
        };

        // Should succeed, because bigGetter.expectedBlockLength() * blocksInMemory = Integer.MAX_VALUE - 8.
        BlockConsumingInputStream.ensureExpectedArraySizeDoesNotOverflow(bigGetter, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void bufferLengthCanNotQuiteReachIntMaxValue() throws IOException {
        BlockGetter reallyBigGetter = new BlockGetter() {
            @Override
            public void get(long firstBlock, long numBlocks, OutputStream destination) {
                // do nothing
            }

            @Override
            public int expectedBlockLength() {
                return (Integer.MAX_VALUE - 1) / 8;
            }
        };

        // Should fail, because reallyBigGetter.expectedBlockLength() * blocksInMemory = Integer.MAX_VALUE - 7.
        int blocksInMemory = 8;
        assertThat((long) reallyBigGetter.expectedBlockLength() * (long) blocksInMemory)
                .as("Test assumption violated: expectedBlockLength() * blocksInMemory > MAX_IN_MEMORY_THRESHOLD.")
                .isGreaterThan(StreamStoreDefinition.MAX_IN_MEMORY_THRESHOLD);
        BlockConsumingInputStream.create(reallyBigGetter, 9, blocksInMemory);
    }

    @Test(expected = IllegalArgumentException.class)
    public void bufferLengthShouldNotExceedMaxArrayLength() throws IOException {
        BlockGetter tooBigGetter = new BlockGetter() {
            @Override
            public void get(long firstBlock, long numBlocks, OutputStream destination) {
                // do nothing
            }

            @Override
            public int expectedBlockLength() {
                return Integer.MAX_VALUE - 7;
            }
        };

        // Should fail, because tooBigGetter.expectedBlockLength() * 1 = Integer.MAX_VALUE - 7.
        BlockConsumingInputStream.create(tooBigGetter, 2, 1);
    }

    @Test
    public void canLoadMultipleBlocksAtOnceAndAlsoFewerBlocksAtEnd() throws IOException {
        BlockGetter spiedGetter = Mockito.spy(singleByteConsumer);
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(spiedGetter, DATA_SIZE, 3);
        stream.read();
        verify(spiedGetter, times(1)).get(anyLong(), eq(3L), any());

        byte[] ata = new byte[3];
        int bytesRead = stream.read(ata);
        assertThat(bytesRead).isEqualTo(3);
        verify(spiedGetter, times(1)).get(anyLong(), eq(1L), any());
    }

    private void expectNextBytesFromStream(BlockConsumingInputStream stream, String expectedOutput) throws IOException {
        byte[] chunk = new byte[2];
        int read = stream.read(chunk);
        assertThat(read).isEqualTo(2);
        assertThat(chunk).isEqualTo(expectedOutput.getBytes(StandardCharsets.UTF_8));
    }
}
