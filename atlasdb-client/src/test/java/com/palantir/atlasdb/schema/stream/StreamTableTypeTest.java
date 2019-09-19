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
package com.palantir.atlasdb.schema.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.List;
import org.junit.Test;

public class StreamTableTypeTest {

    private static final String TEST_TABLE = "test";
    private static final Namespace TEST_NAMESPACE = Namespace.create("namespace");

    @Test
    public void isStreamStoreValueTableReturnsTrueForTableWithEmptyNamespace() {
        String tableName = StreamTableType.VALUE.getTableName(TEST_TABLE);
        TableReference tableReference = TableReference.createWithEmptyNamespace(tableName);
        assertTrue(StreamTableType.isStreamStoreValueTable(tableReference));
    }

    @Test
    public void isStreamStoreValueTableReturnsTrueForTableWithNamespace() {
        String tableName = StreamTableType.VALUE.getTableName(TEST_TABLE);
        TableReference tableReference = TableReference.create(TEST_NAMESPACE, tableName);
        assertTrue(StreamTableType.isStreamStoreValueTable(tableReference));
    }

    @Test
    public void isStreamStoreValueTableReturnsFalseForTestTable() {
        TableReference tableReference = TableReference.createWithEmptyNamespace(TEST_TABLE);
        assertFalse(StreamTableType.isStreamStoreValueTable(tableReference));
    }

    @Test
    public void isStreamStoreValueTableReturnsFalseForOtherStreamStoreTableTypes() {
        List<StreamTableType> streamTypes =
                Lists.newArrayList(StreamTableType.METADATA, StreamTableType.INDEX, StreamTableType.HASH);
        for (StreamTableType streamType : streamTypes) {
            String tableName = streamType.getTableName(TEST_TABLE);
            TableReference tableReference = TableReference.create(TEST_NAMESPACE, tableName);

            assertFalse(StreamTableType.isStreamStoreValueTable(tableReference));
        }
    }

    @Test
    public void getIndexTableFromValueTableWorksWithTableWithEmptyNamespace() {
        String valueTableName = StreamTableType.VALUE.getTableName(TEST_TABLE);
        TableReference valueTable = TableReference.createWithEmptyNamespace(valueTableName);

        String indexTableName = StreamTableType.INDEX.getTableName(TEST_TABLE);
        TableReference expectedIndexTable = TableReference.createWithEmptyNamespace(indexTableName);

        TableReference indexTableFromValueTable = StreamTableType.getIndexTableFromValueTable(valueTable);
        assertThat(indexTableFromValueTable).isNotEqualTo(valueTable);
        assertEquals(expectedIndexTable, indexTableFromValueTable);
    }

    @Test
    public void getIndexTableFromValueTableWorksWithTableWithNamespace() {
        String valueTableName = StreamTableType.VALUE.getTableName(TEST_TABLE);
        TableReference valueTable = TableReference.create(TEST_NAMESPACE, valueTableName);

        String indexTableName = StreamTableType.INDEX.getTableName(TEST_TABLE);
        TableReference expectedIndexTable = TableReference.create(TEST_NAMESPACE, indexTableName);

        TableReference indexTableFromValueTable = StreamTableType.getIndexTableFromValueTable(valueTable);
        assertThat(indexTableFromValueTable).isNotEqualTo(valueTable);
        assertEquals(expectedIndexTable, indexTableFromValueTable);
    }
}
