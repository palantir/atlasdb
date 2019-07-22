/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.TransactionTestSetup;

public class LiveMigratorTest extends TransactionTestSetup {
    private static final TableReference OLD_TABLE_REF = TableReference.createFromFullyQualifiedName("old.table");
    private static final TableReference NEW_TABLE_REF = TableReference.createFromFullyQualifiedName("new.table");
    private static final Cell CELL = createCell("rowName", "columnName");

    @ClassRule
    public static final TestResourceManager TRM = TestResourceManager.inMemory();

    private KeyValueService kvs;
    private TransactionManager transactionManager;

    private final ProgressCheckPoint checkPoint = mock(ProgressCheckPoint.class);

    private LiveMigrator liveMigrator;

    public LiveMigratorTest() {
        super(TRM, TRM);
    }

    @Before
    public void before() {
        kvs = TRM.getDefaultKvs();
        kvs.createTable(OLD_TABLE_REF, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(NEW_TABLE_REF, AtlasDbConstants.GENERIC_TABLE_METADATA);
        transactionManager = getManager();
        liveMigrator = new LiveMigrator(transactionManager, OLD_TABLE_REF, NEW_TABLE_REF, checkPoint);
    }

    @Test
    public void testMigrationRuns() {
        transactionManager.runTaskWithRetry(transaction -> {
            transaction.put(OLD_TABLE_REF,
                    ImmutableMap.of(CELL, PtBytes.toBytes("value")));

            return null;
        });


        byte[] value = transactionManager.runTaskWithRetry(
                transaction -> transaction.get(OLD_TABLE_REF, ImmutableSet.of(CELL)).get(CELL));

        assertThat(value)
                .containsExactly(PtBytes.toBytes("value"));

        when(checkPoint.getNextStartRow())
                .thenReturn(Optional.of(PtBytes.EMPTY_BYTE_ARRAY))
                .thenReturn(Optional.empty());

        liveMigrator.startMigration();

        byte[] valueInNewTable = transactionManager.runTaskWithRetry(
                transaction -> transaction.get(NEW_TABLE_REF, ImmutableSet.of(CELL)).get(CELL));

        assertThat(valueInNewTable)
                .containsExactly(PtBytes.toBytes("value"));

    }

    private static Cell createCell(String rowName, String columnName) {
        return Cell.create(PtBytes.toBytes(rowName), PtBytes.toBytes(columnName));
    }
}