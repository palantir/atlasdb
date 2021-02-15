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
package com.palantir.atlasdb.keyvalue.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AbstractMultiCasTest {
    private static final byte[] ROW_1 = PtBytes.toBytes("row1");
    private static final byte[] COL_1 = PtBytes.toBytes("col1");
    public static final Cell FIRST_CELL = Cell.create(ROW_1, COL_1);
    private static final byte[] COL_2 = PtBytes.toBytes("col2");
    private static final byte[] COL_3 = PtBytes.toBytes("col3");
    private static final byte[] DATA = PtBytes.toBytes("data");

    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("test.table");

    private final KvsManager kvsManager;
    private KeyValueService kvs;

    public AbstractMultiCasTest(KvsManager kvsManager) {
        this.kvsManager = kvsManager;
    }

    @Before
    public void setup() {
        kvs = kvsManager.getDefaultKvs();
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @After
    public void cleanup() {
        kvs.truncateTable(TEST_TABLE);
    }

    @Test
    public void atomicallyFailsForSingleRowWhenColumnExists() {
        kvs.put(TEST_TABLE, ImmutableMap.of(Cell.create(ROW_1, COL_3), DATA), 0);
        assertThatThrownBy(() -> kvs.putUnlessExists(TEST_TABLE, putDataInCols(COL_1, COL_2, COL_3)))
                .isInstanceOf(KeyAlreadyExistsException.class);

        assertThat(kvs.get(TEST_TABLE, ImmutableMap.of(FIRST_CELL, Long.MAX_VALUE)))
                .isEmpty();
    }

    @Test
    public void suceedsWhenThereIsDataInOtherColumns() {
        kvs.put(TEST_TABLE, ImmutableMap.of(Cell.create(ROW_1, COL_3), DATA), 0);
        kvs.putUnlessExists(TEST_TABLE, putDataInCols(COL_1, COL_2));

        assertThat(kvs.get(TEST_TABLE, ImmutableMap.of(FIRST_CELL, Long.MAX_VALUE))
                        .get(FIRST_CELL)
                        .getContents())
                .contains(DATA);
    }

    private static Map<Cell, byte[]> putDataInCols(byte[]... cols) {
        return Stream.of(cols)
                .map(column -> Cell.create(ROW_1, column))
                .collect(Collectors.toMap(cell -> cell, ignore -> DATA));
    }
}
