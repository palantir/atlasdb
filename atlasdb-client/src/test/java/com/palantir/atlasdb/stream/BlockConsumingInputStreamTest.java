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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.Test;
import org.mockito.Mockito;

public class BlockConsumingInputStreamTest {
    private static final int DATA_SIZE = 4;
    private static final int DATA_SIZE_PLUS_ONE = 5;

    private final byte[] data = "data".getBytes();
    private final BlockGetter dataConsumer = new BlockGetter() {
        @Override
        public void get(Integer firstBlock, Integer numBlocks, OutputStream destination) {
            try {
                destination.write(data);
            } catch (IOException e) {
                fail();
            }
        }

        @Override
        public int expectedLength() {
            return data.length;
        }
    };

    private final BlockGetter singleByteConsumer = new BlockGetter() {
        @Override
        public void get(Integer offset, Integer numBlocks, OutputStream os) {
            try {
                os.write(data, offset, numBlocks);
            } catch (IOException e) {
                fail();
            }
        }

        @Override
        public int expectedLength() {
            return data.length;
        }
    };

    private final byte[] stored = "divisible".getBytes();
    private final BlockGetter threeByteConsumer = new BlockGetter() {
        @Override
        public void get(Integer offset, Integer numBlocks, OutputStream os) {
            try {
                os.write(stored, 3 * offset, 3 * numBlocks);
            } catch (IOException e) {
                fail();
            }
        }

        @Override
        public int expectedLength() {
            return data.length;
        }
    };

    @Test
    public void can_read_single_byte() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(dataConsumer, 1, 1);
        int byteAsInt = stream.read();
        byte[] readByte = { (byte) byteAsInt };
        assertEquals("d", new String(readByte, StandardCharsets.UTF_8));
    }

    @Test
    public void read_empty_array_returns_zero() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(dataConsumer, 1, 1);
        int read = stream.read(new byte[0]);
        assertEquals(0, read);
    }

    @Test
    public void can_read_block() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(dataConsumer, 1, 1);
        byte[] result = new byte[DATA_SIZE];
        int read = stream.read(result);
        assertEquals(DATA_SIZE, read);
        assertArrayEquals(data, result);
    }

    @Test
    public void can_read_across_blocks() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(threeByteConsumer, 2, 1);
        expectNextBytesFromStream(stream, "di");
        expectNextBytesFromStream(stream, "vi");
        expectNextBytesFromStream(stream, "si");
    }

    @Test
    public void can_read_across_blocks_with_incomplete_final_block() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(threeByteConsumer, 3, 2);
        expectNextBytesFromStream(stream, "di");
        expectNextBytesFromStream(stream, "vi");
        expectNextBytesFromStream(stream, "si");
        expectNextBytesFromStream(stream, "bl");

        byte[] chunk = new byte[2];
        int read = stream.read(chunk);
        assertEquals(1, read);
        assertArrayEquals("e".getBytes(), Arrays.copyOf(chunk, 1));
    }

    @Test
    public void read_single_byte_when_stream_exhausted_returns_minus_one() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(dataConsumer, 1, 1);
        //noinspection ResultOfMethodCallIgnored
        stream.read(new byte[DATA_SIZE]);

        int read = stream.read();
        assertEquals(-1, read);
    }

    @Test
    public void read_when_stream_exhausted_returns_minus_one() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(dataConsumer, 1, 1);
        byte[] result = new byte[DATA_SIZE];
        //noinspection ResultOfMethodCallIgnored
        stream.read(result);

        int read = stream.read(result);
        assertEquals(-1, read);
    }

    @Test
    public void larger_arrays_than_data_get_partially_filled() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(dataConsumer, 1, 1);
        byte[] result = new byte[DATA_SIZE_PLUS_ONE];
        int read = stream.read(result);
        assertEquals(DATA_SIZE, read);
        assertArrayEquals(data, Arrays.copyOf(result, DATA_SIZE));
    }

    @Test
    public void can_read_multiple_blocks() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(singleByteConsumer, DATA_SIZE, 1);
        byte[] result = new byte[DATA_SIZE];
        int read = stream.read(result);
        assertEquals(DATA_SIZE, read);
        assertArrayEquals(data, result);
    }

    @Test
    public void can_read_multiple_blocks_and_partially_fill_result() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(singleByteConsumer, DATA_SIZE, 1);
        byte[] result = new byte[DATA_SIZE_PLUS_ONE];
        int read = stream.read(result);
        assertEquals(DATA_SIZE, read);
        assertArrayEquals(data, Arrays.copyOf(result, DATA_SIZE));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void passing_in_too_many_blocks_causes_an_exception() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(singleByteConsumer, DATA_SIZE_PLUS_ONE, 1);
        byte[] result = new byte[DATA_SIZE_PLUS_ONE];
        //noinspection ResultOfMethodCallIgnored
        stream.read(result);
    }

    @Test
    public void passing_in_too_few_blocks_causes_incomplete_output() throws IOException {
        int dataSizeMinusOne = 3;
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(singleByteConsumer, dataSizeMinusOne, 1);
        byte[] result = new byte[DATA_SIZE];
        int read = stream.read(result);
        assertEquals(dataSizeMinusOne, read);
        assertArrayEquals(Arrays.copyOf(data, dataSizeMinusOne), Arrays.copyOf(result, dataSizeMinusOne));
    }

    @Test
    public void can_load_multiple_blocks_at_once_and_also_fewer_blocks_at_end() throws IOException {
        BlockGetter spiedGetter = Mockito.spy(new MockableBlockGetter(singleByteConsumer));
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(spiedGetter, DATA_SIZE, 3);
        //noinspection ResultOfMethodCallIgnored
        stream.read();
        verify(spiedGetter, times(1)).get(anyInt(), eq(3), any());

        byte[] ata = new byte[3];
        int bytesRead = stream.read(ata);
        assertEquals(3, bytesRead);
        verify(spiedGetter, times(1)).get(anyInt(), eq(1), any());
    }

    private void expectNextBytesFromStream(BlockConsumingInputStream stream, String expectedOutput) throws IOException {
        byte[] chunk = new byte[2];
        int read = stream.read(chunk);
        assertEquals(2, read);
        assertArrayEquals(expectedOutput.getBytes(), chunk);
    }

    private class MockableBlockGetter implements BlockGetter {
        private BlockGetter delegate;

        MockableBlockGetter(BlockGetter delegate) {
            this.delegate = delegate;
        }

        @Override
        public void get(Integer firstBlock, Integer numBlocks, OutputStream destination) {
            delegate.get(firstBlock, numBlocks, destination);
        }

        @Override
        public int expectedLength() {
            return delegate.expectedLength();
        }
    }
}
