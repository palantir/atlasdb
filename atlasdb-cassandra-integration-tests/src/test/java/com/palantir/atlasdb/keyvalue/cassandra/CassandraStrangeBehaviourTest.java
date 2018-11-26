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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RetryLimitReachedException;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class CassandraStrangeBehaviourTest {
    private static final TableReference UPPER_UPPER = TableReference.createFromFullyQualifiedName("TEST.TABLE");
    private static final TableReference LOWER_UPPER = TableReference.createFromFullyQualifiedName("test.TABLE");
    private static final TableReference LOWER_LOWER = TableReference.createFromFullyQualifiedName("test.table");
    private static final List<TableReference> TABLES = ImmutableList.of(UPPER_UPPER, LOWER_UPPER, LOWER_LOWER);
    public static final byte[] BYTE_ARRAY = new byte[] {1};
    public static final byte[] SECOND_BYTE_ARRAY = new byte[] {2};
    public static final Cell CELL = Cell.create(BYTE_ARRAY, BYTE_ARRAY);

    private KeyValueService kvs = CASSANDRA.getDefaultKvs();

    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    @After
    public void cleanup() {
        kvs.dropTables(kvs.getAllTableNames());
        kvs.truncateTable(AtlasDbConstants.DEFAULT_METADATA_TABLE);
    }

    @Test
    public void createTablesWithDifferentCapitalizationAndNoMetadata() {
        createTablesIgnoringException();

        assertThat(kvs.getAllTableNames()).containsExactlyInAnyOrderElementsOf(TABLES);
        assertThat(kvs.getMetadataForTables()).isEmpty();
    }

    @Test
    public void tablesWithDifferentCapitalizationClaimToHaveMetadata() {
        kvs.createTable(UPPER_UPPER, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(LOWER_UPPER, AtlasDbConstants.GENERIC_TABLE_METADATA);

        TABLES.forEach(table -> assertThat(kvs.getMetadataForTable(table)).isNotEmpty());
        assertThat(kvs.getMetadataForTable(TableReference.createFromFullyQualifiedName("other.table"))).isEmpty();
    }

    @Test
    public void getMetadataMapsToLowerCasedTableRef() {
        kvs.createTable(UPPER_UPPER, AtlasDbConstants.GENERIC_TABLE_METADATA);

        assertThat(kvs.getMetadataForTables().keySet())
                .contains(UPPER_UPPER)
                .doesNotContain(LOWER_LOWER);
    }

    @Test
    public void droppedTableMetadataDoesNotShowUpInGetMetadataForTables() {
        kvs.createTable(LOWER_UPPER, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.dropTable(LOWER_UPPER);

        assertThat(kvs.getMetadataForTables().keySet()).isEmpty();
    }

    @Test
    public void tablesAreActuallyCaseSensitive() {
        createTablesIgnoringException();

        kvs.put(UPPER_UPPER, ImmutableMap.of(CELL, BYTE_ARRAY), 1);
        kvs.put(LOWER_LOWER, ImmutableMap.of(CELL, SECOND_BYTE_ARRAY), 1);

        assertThat(kvs.get(UPPER_UPPER, ImmutableMap.of(CELL, 2L)).get(CELL).getContents()).contains(BYTE_ARRAY);
        assertThat(kvs.get(LOWER_UPPER, ImmutableMap.of(CELL, 2L))).doesNotContainKey(CELL);
        assertThat(kvs.get(LOWER_LOWER, ImmutableMap.of(CELL, 2L)).get(CELL).getContents()).contains(SECOND_BYTE_ARRAY);
    }

    @Test
    public void weTellYouTableIsCreatedButWeLie() {
        kvs.createTable(UPPER_UPPER, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(LOWER_LOWER, AtlasDbConstants.GENERIC_TABLE_METADATA);

        kvs.put(UPPER_UPPER, ImmutableMap.of(CELL, BYTE_ARRAY), 1);
        assertThat(kvs.get(UPPER_UPPER, ImmutableMap.of(CELL, 2L)).get(CELL).getContents()).contains(BYTE_ARRAY);

        assertThatThrownBy(() -> kvs.get(LOWER_LOWER, ImmutableMap.of(CELL, 2L)))
                .isInstanceOf(RetryLimitReachedException.class);

        assertThatThrownBy(() -> kvs.put(LOWER_LOWER, ImmutableMap.of(CELL, SECOND_BYTE_ARRAY), 1))
                .isInstanceOf(RetryLimitReachedException.class);
    }

    private void createTablesIgnoringException() {
        assertThatThrownBy(() -> kvs.createTables(
                TABLES.stream().collect(Collectors.toMap(x -> x, no -> AtlasDbConstants.GENERIC_TABLE_METADATA))))
                .isInstanceOf(IllegalStateException.class);
    }
}
