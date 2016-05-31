/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.transaction.impl;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.stream.StreamTest;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class SnapshotTransactionIntegrationTest extends AtlasDbTestCase {

    @Before
    public void createSchema() {
        Schemas.deleteTablesAndIndexes(schema(), keyValueService);
        Schemas.createTablesAndIndexes(schema(), keyValueService);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldForbidEmptyRows() {
        TableReference table = TableReference.create(Namespace.DEFAULT_NAMESPACE, BLOB_ONLY_TABLE_NAME);

        putCells(table, ImmutableMap.of(blobCell, PtBytes.EMPTY_BYTE_ARRAY));
    }

    @Test
    public void rowsWithEmptyObjectsShouldBeVisible() {
        TableReference table = TableReference.create(Namespace.DEFAULT_NAMESPACE, BLOB_AND_VALUE_TABLE_NAME);
        putCells(table, ImmutableMap.of(otherCell, PtBytes.toBytes(123L)));

        RowResult<byte[]> rowResult = getRowResult(table, blobCell.getRowName());

        assertThat(rowResult, is(notNullValue()));
        assertThat(rowResult.getCellSet(), hasItem(otherCell));
        assertThat(rowResult.getCellSet(), not(hasItem(blobCell)));
    }

    private void putCells(final TableReference table, final ImmutableMap<Cell, byte[]> cells) {
        txManager.runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) throws RuntimeException {
                t.put(table, cells);
                t.commit();
                return null;
            }
        });
    }

    private RowResult<byte[]> getRowResult(final TableReference table, final byte[] rowName) {
        return txManager.runTaskWithRetry(new TransactionTask<RowResult<byte[]>, RuntimeException>() {
                @Override
                public RowResult<byte[]> execute(Transaction t) throws RuntimeException {
                    return t.getRows(table, ImmutableSet.of(rowName), ColumnSelection.all()).get(rowName);
                }
            });
    }

    private static final String BLOB_ONLY_TABLE_NAME = "blobsOnly";
    private static final String BLOB_AND_VALUE_TABLE_NAME = "blobsAndLongs";

    private static final Cell blobCell = Cell.create("id".getBytes(Charsets.UTF_8), "blob".getBytes(Charsets.UTF_8));
    private static final Cell otherCell = Cell.create("id".getBytes(Charsets.UTF_8), "timestamp".getBytes(Charsets.UTF_8));


    private Schema schema() {
        Schema schema = new Schema("TransactionTest",
                StreamTest.class.getPackage().getName() + ".generated",
                Namespace.DEFAULT_NAMESPACE);

        schema.addTableDefinition(BLOB_ONLY_TABLE_NAME, new TableDefinition() {{
            rowName();
            rowComponent("id", ValueType.STRING);

            columns();
            column("blob", "b", ValueType.BLOB);
        }});

        schema.addTableDefinition(BLOB_AND_VALUE_TABLE_NAME, new TableDefinition() {{
            rowName();
            rowComponent("id", ValueType.STRING);

            columns();
            column("blob", "b", ValueType.BLOB);
            column("timestamp", "t", ValueType.VAR_LONG);
        }});

        return schema;
    }
}
