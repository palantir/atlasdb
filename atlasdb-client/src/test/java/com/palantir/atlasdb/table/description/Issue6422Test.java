/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.table.description;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.table.description.generated.GenericTestSchemaTableFactory;
import com.palantir.atlasdb.table.description.generated.Issue6422Table;
import com.palantir.atlasdb.table.description.generated.Issue6422Table.Issue6422Column;
import com.palantir.atlasdb.table.description.generated.Issue6422Table.Issue6422ColumnValue;
import com.palantir.atlasdb.table.description.generated.Issue6422Table.Issue6422Row;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class Issue6422Test {

    private TransactionManager txnManager;

    @Before
    public void before() {
        txnManager = TransactionManagers.createInMemory(GenericTestSchema.getSchema());
    }

    @After
    public void after() {
        txnManager.close();
    }

    @Test
    public void test() {
        txnManager.runTaskThrowOnConflict(txn -> {
            Issue6422Table table = GenericTestSchemaTableFactory.of().getIssue6422Table(txn);

            table.put(Issue6422Row.of(0), Issue6422ColumnValue.of(Issue6422Column.of(0), 42L));

            // The write is visible in getRowColumns
            assertThat(table.getRowColumns(Issue6422Row.of(0))).hasSize(1);

            // But the write is not visible in getRowsColumnRange
            assertThat(table.getRowsColumnRange(List.of(Issue6422Row.of(0)), new ColumnRangeSelection(null, null), 10))
                    .toIterable()
                    .hasSize(0);

            return null;
        });

        // The write is visible in a new transaction
        txnManager.runTaskThrowOnConflict(txn -> {
            Issue6422Table table = GenericTestSchemaTableFactory.of().getIssue6422Table(txn);

            assertThat(table.getRowColumns(Issue6422Row.of(0))).hasSize(1);

            assertThat(table.getRowsColumnRange(List.of(Issue6422Row.of(0)), new ColumnRangeSelection(null, null), 10))
                    .toIterable()
                    .hasSize(1);

            return null;
        });
    }
}
