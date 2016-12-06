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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.BiConsumer;

import org.junit.Test;

public class BlockConsumingInputStreamTest {
    private static final int DATA_SIZE = 4;
    private static final int DATA_SIZE_PLUS_ONE = 5;

    private final byte[] data = "data".getBytes();
    private final BiConsumer<Integer, OutputStream> dataConsumer = (block, os) -> {
        try {
            os.write(data);
        } catch (IOException e) {
            fail();
        }
    };
    private final BiConsumer<Integer, OutputStream> singleByteConsumer = (block, os) -> {
        try {
            os.write(data[block]);
        } catch (IOException e) {
            fail();
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
    public void can_read_block() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(dataConsumer, 1, 1);
        byte[] result = new byte[DATA_SIZE];
        int read = stream.read(result);
        assertEquals(DATA_SIZE, read);
        assertArrayEquals(data, result);
    }

    @Test
    public void larger_arrays_than_data_get_partially_filled() throws IOException {
        BlockConsumingInputStream stream = BlockConsumingInputStream.create(dataConsumer, 1,1 );
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

    @Test(expected = ArrayIndexOutOfBoundsException.class)
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
}