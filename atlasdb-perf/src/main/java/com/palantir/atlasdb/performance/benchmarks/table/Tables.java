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

package com.palantir.atlasdb.performance.benchmarks.table;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public final class Tables {

    static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName("p.tab");

    static final String ROW_COMPONENT = "key";
    public static final ByteBuffer ROW_BYTES = ByteBuffer.wrap(ROW_COMPONENT.getBytes(StandardCharsets.UTF_8));

    static final String COLUMN_NAME = "value";
    public static final ByteBuffer COLUMN_NAME_IN_BYTES = ByteBuffer.wrap(COLUMN_NAME.getBytes(StandardCharsets.UTF_8));
    static final String COLUMN_COMPONENT = "col";

    static final long DUMMY_TIMESTAMP = 1L;

    static final long RANDOM_SEED = 279L;

    private static final int VALUE_BYTE_ARRAY_SIZE = 100;
    private static final int KEY_BYTE_ARRAY_SIZE = 32;

    private Tables() {
        // uninstantiable
    }

    private static byte[] generateValue(Random random) {
        byte[] value = new byte[Tables.VALUE_BYTE_ARRAY_SIZE];
        random.nextBytes(value);
        return value;
    }

    private static byte[] generateKey(Random random) {
        byte[] key = new byte[Tables.KEY_BYTE_ARRAY_SIZE];
        random.nextBytes(key);
        return key;
    }

    static Map<Cell, byte[]> generateRandomBatch(Random random, int size) {
        Map<Cell, byte[]> map = Maps.newHashMapWithExpectedSize(size);
        for (int j = 0; j < size; j++) {
            byte[] key = generateKey(random);
            byte[] value = generateValue(random);
            map.put(Cell.create(key, Tables.COLUMN_NAME_IN_BYTES.array()), value);
        }
        return map;
    }

    static Map<Cell, byte[]> generateContinuousBatch(Random random, int startKey, int size) {
        Map<Cell, byte[]> map = Maps.newHashMapWithExpectedSize(size);
        for (int j = 0; j < size; j++) {
            byte[] key = Ints.toByteArray(startKey + j);
            byte[] value = Tables.generateValue(random);
            map.put(Cell.create(key, Tables.COLUMN_NAME_IN_BYTES.array()), value);
        }
        return map;
    }

}
