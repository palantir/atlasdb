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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Test;

@SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly"}) // Expectations syntax
public class TableSplittingKeyValueServiceTest {
    private static final Namespace NAMESPACE = Namespace.create("namespace");
    private static final TableReference TABLE = TableReference.create(NAMESPACE, "table");
    private static final Cell CELL =
            Cell.create("row".getBytes(StandardCharsets.UTF_8), "column".getBytes(StandardCharsets.UTF_8));
    private static final byte[] VALUE = "value".getBytes(StandardCharsets.UTF_8);
    private static final long TIMESTAMP = 123L;
    public static final ImmutableMap<Cell, byte[]> VALUES = ImmutableMap.of(CELL, VALUE);

    private final Mockery mockery = new Mockery();
    private final KeyValueService tableDelegate = mockery.mock(KeyValueService.class, "table delegate");
    private final KeyValueService otherTableDelegate = mockery.mock(KeyValueService.class, "other table delegate");
    private final KeyValueService namespaceDelegate = mockery.mock(KeyValueService.class, "namespace delegate");
    private final KeyValueService defaultKvs = mockery.mock(KeyValueService.class, "default kvs");

    @Test
    public void delegatesMethodsToTheKvsAssociatedWithTheTable() {
        TableSplittingKeyValueService splittingKvs = TableSplittingKeyValueService.create(
                ImmutableList.of(defaultKvs, tableDelegate), ImmutableMap.of(TABLE, tableDelegate));

        mockery.checking(new Expectations() {
            {
                oneOf(tableDelegate).put(TABLE, VALUES, TIMESTAMP);
            }
        });

        splittingKvs.put(TABLE, VALUES, TIMESTAMP);
    }

    @Test
    public void delegatesMethodsToTheKvsAssociatedWithTheNamespaceIfNoTableMappingExists() {
        TableSplittingKeyValueService splittingKvs = TableSplittingKeyValueService.create(
                ImmutableList.of(tableDelegate, namespaceDelegate),
                ImmutableMap.of(),
                ImmutableMap.of(NAMESPACE, namespaceDelegate));

        mockery.checking(new Expectations() {
            {
                oneOf(namespaceDelegate).put(TABLE, VALUES, TIMESTAMP);
            }
        });

        splittingKvs.put(TABLE, VALUES, TIMESTAMP);
    }

    @Test
    public void prioritisesTableDelegatesOverNamespaceDelegates() {
        TableSplittingKeyValueService splittingKvs = TableSplittingKeyValueService.create(
                ImmutableList.of(tableDelegate, namespaceDelegate),
                ImmutableMap.of(TABLE, tableDelegate),
                ImmutableMap.of(NAMESPACE, namespaceDelegate));

        mockery.checking(new Expectations() {
            {
                oneOf(tableDelegate).put(TABLE, VALUES, TIMESTAMP);
            }
        });

        splittingKvs.put(TABLE, VALUES, TIMESTAMP);
    }

    @Test
    public void defaultsToTheFirstKvsInTheListIfNoMappingsMatch() {
        TableSplittingKeyValueService splittingKvs = TableSplittingKeyValueService.create(
                ImmutableList.of(defaultKvs, tableDelegate),
                ImmutableMap.of(TableReference.createWithEmptyNamespace("not-this"), tableDelegate));

        mockery.checking(new Expectations() {
            {
                oneOf(defaultKvs).put(TABLE, VALUES, TIMESTAMP);
            }
        });

        splittingKvs.put(TABLE, VALUES, TIMESTAMP);
    }

    @Test
    public void splitsTableMetadataIntoTheCorrectTables() {
        TableReference table1 = TableReference.createWithEmptyNamespace("table1");
        TableReference table2 = TableReference.createWithEmptyNamespace("table2");
        TableReference table3 = TableReference.createWithEmptyNamespace("table3");
        TableSplittingKeyValueService splittingKvs = TableSplittingKeyValueService.create(
                ImmutableList.of(tableDelegate, otherTableDelegate),
                ImmutableMap.of(
                        table1, tableDelegate,
                        table2, otherTableDelegate,
                        table3, otherTableDelegate));

        final ImmutableMap<TableReference, byte[]> tableSpec1 =
                ImmutableMap.of(table1, "1".getBytes(StandardCharsets.UTF_8));
        final ImmutableMap<TableReference, byte[]> tableSpec2 = ImmutableMap.of(
                table2, "2".getBytes(StandardCharsets.UTF_8),
                table3, "3".getBytes(StandardCharsets.UTF_8));

        mockery.checking(new Expectations() {
            {
                oneOf(tableDelegate).createTables(tableSpec1);
                oneOf(otherTableDelegate).createTables(tableSpec2);
            }
        });

        splittingKvs.createTables(merge(tableSpec1, tableSpec2));
    }

    @Test
    public void evaluatesCheckAndSetCompatibilityCorrectly() {
        TableSplittingKeyValueService splittingKvs = TableSplittingKeyValueService.create(
                ImmutableList.of(defaultKvs, tableDelegate), ImmutableMap.of(TABLE, tableDelegate));

        mockery.checking(new Expectations() {
            {
                oneOf(defaultKvs).getCheckAndSetCompatibility();
                will(returnValue(CheckAndSetCompatibility.SUPPORTS_DETAIL_NOT_CONSISTENT_ON_FAILURE));
                oneOf(tableDelegate).getCheckAndSetCompatibility();
                will(returnValue(CheckAndSetCompatibility.SUPPORTS_DETAIL_CONSISTENT_ON_FAILURE));
            }
        });
        assertThat(splittingKvs.getCheckAndSetCompatibility())
                .isEqualTo(CheckAndSetCompatibility.SUPPORTS_DETAIL_NOT_CONSISTENT_ON_FAILURE);
    }

    private Map<TableReference, byte[]> merge(
            ImmutableMap<TableReference, byte[]> left, ImmutableMap<TableReference, byte[]> right) {
        return ImmutableMap.<TableReference, byte[]>builder()
                .putAll(left)
                .putAll(right)
                .build();
    }
}
