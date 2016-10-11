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
package com.palantir.atlasdb.schema.indexing;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.hash.Hashing;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.schema.indexing.generated.DataTable;
import com.palantir.atlasdb.schema.indexing.generated.DataTable.Index1IdxTable.Index1IdxRow;
import com.palantir.atlasdb.schema.indexing.generated.IndexTestTableFactory;
import com.palantir.atlasdb.schema.indexing.generated.TwoColumnsTable;
import com.palantir.atlasdb.schema.indexing.generated.TwoColumnsTable.Bar;
import com.palantir.atlasdb.schema.indexing.generated.TwoColumnsTable.Foo;
import com.palantir.atlasdb.schema.indexing.generated.TwoColumnsTable.FooToIdIdxTable;
import com.palantir.atlasdb.schema.indexing.generated.TwoColumnsTable.FooToIdIdxTable.FooToIdIdxRowResult;
import com.palantir.atlasdb.schema.indexing.generated.TwoColumnsTable.TwoColumnsNamedColumnValue;
import com.palantir.atlasdb.schema.indexing.generated.TwoColumnsTable.TwoColumnsRow;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.RuntimeTransactionTask;
import com.palantir.atlasdb.transaction.api.Transaction;

public class IndexTest extends AtlasDbTestCase {

    @Before
    public void createSchema() {
        Schemas.truncateTablesAndIndexes(IndexTestSchema.getSchema(), keyValueService);
        Schemas.createTablesAndIndexes(IndexTestSchema.getSchema(), keyValueService);
    }

    @Test
    public void testAddDelete() {
        txManager.runTaskWithRetry(new RuntimeTransactionTask<Void>() {
            @Override
            public Void execute(Transaction t) {
                DataTable table = getTableFactory().getDataTable(t);
                table.putValue(DataTable.DataRow.of(1L), 2L);
                table.putValue(DataTable.DataRow.of(3L), 2L);
                return null;
            }
        });
        txManager.runTaskWithRetry(new RuntimeTransactionTask<Void>() {
            @Override
            public Void execute(Transaction t) {
                DataTable.Index1IdxTable index1 = DataTable.Index1IdxTable.of(getTableFactory().getDataTable(t));
                DataTable.Index2IdxTable index2 = DataTable.Index2IdxTable.of(getTableFactory().getDataTable(t));
                DataTable.Index3IdxTable index3 = DataTable.Index3IdxTable.of(getTableFactory().getDataTable(t));
                assert index1.getRange(RangeRequest.builder().build()).count() == 1;
                assert index2.getRange(RangeRequest.builder().build()).count() == 2;
                assert index3.getRange(RangeRequest.builder().build()).count() == 1;
                return null;
            }
        });
        txManager.runTaskWithRetry(new RuntimeTransactionTask<Void>() {
            @Override
            public Void execute(Transaction t) {
                DataTable table = getTableFactory().getDataTable(t);
                table.delete(DataTable.DataRow.of(1L));
                return null;
            }
        });
        txManager.runTaskWithRetry(new RuntimeTransactionTask<Void>() {
            @Override
            public Void execute(Transaction t) {
                DataTable.Index1IdxTable index1 = DataTable.Index1IdxTable.of(getTableFactory().getDataTable(t));
                DataTable.Index2IdxTable index2 = DataTable.Index2IdxTable.of(getTableFactory().getDataTable(t));
                DataTable.Index3IdxTable index3 = DataTable.Index3IdxTable.of(getTableFactory().getDataTable(t));
                assert index1.getRange(RangeRequest.builder().build()).count() == 1;
                assert index2.getRange(RangeRequest.builder().build()).count() == 1;
                assert index3.getRange(RangeRequest.builder().build()).count() == 1;
                return null;
            }
        });
    }

    @Test
    public void testUpdate() {
        txManager.runTaskWithRetry(new RuntimeTransactionTask<Void>() {
            @Override
            public Void execute(Transaction t) {
                DataTable table = getTableFactory().getDataTable(t);
                table.putValue(DataTable.DataRow.of(1L), 2L);
                return null;
            }
        });
        txManager.runTaskWithRetry(new RuntimeTransactionTask<Void>() {
            @Override
            public Void execute(Transaction t) {
                DataTable.Index1IdxTable index1 = DataTable.Index1IdxTable.of(getTableFactory().getDataTable(t));
                assert Iterables.getOnlyElement(index1.getRowColumns(Index1IdxRow.of(2L))).getColumnName().getId() == 1L;
                return null;
            }
        });
        txManager.runTaskWithRetry(new RuntimeTransactionTask<Void>() {
            @Override
            public Void execute(Transaction t) {
                DataTable table = getTableFactory().getDataTable(t);
                table.putValue(DataTable.DataRow.of(1L), 3L);
                return null;
            }
        });
        txManager.runTaskWithRetry(new RuntimeTransactionTask<Void>() {
            @Override
            public Void execute(Transaction t) {
                DataTable.Index1IdxTable index1 = DataTable.Index1IdxTable.of(getTableFactory().getDataTable(t));
                assert index1.getRowColumns(Index1IdxRow.of(2L)).isEmpty();
                return null;
            }
        });
    }

    @Test
    public void testTwoColumns() {
        txManager.runTaskWithRetry(new RuntimeTransactionTask<Void>() {
            @Override
            public Void execute(Transaction t) {
                TwoColumnsTable table = getTableFactory().getTwoColumnsTable(t);
                Multimap<TwoColumnsRow, TwoColumnsNamedColumnValue<?>> rows = HashMultimap.create();
                TwoColumnsRow key = TwoColumnsRow.of(1L);
                rows.put(key, Foo.of(2L));
                rows.put(key, Bar.of(5L));
                table.put(rows);
                return null;
            }
        });
        txManager.runTaskWithRetry(new RuntimeTransactionTask<Void>() {
            @Override
            public Void execute(Transaction t) {
                TwoColumnsTable table = getTableFactory().getTwoColumnsTable(t);
                table.putBar(TwoColumnsRow.of(1L), 6L);
                return null;
            }
        });
        txManager.runTaskWithRetry(new RuntimeTransactionTask<Void>() {
            @Override
            public Void execute(Transaction t) {
                FooToIdIdxTable index = FooToIdIdxTable.of(getTableFactory().getTwoColumnsTable(t));
                List<FooToIdIdxRowResult> result = index.getAllRowsUnordered().immutableCopy();
                Assert.assertEquals(2L, Iterables.getOnlyElement(result).getRowName().getFoo());
                return null;
            }
        });
    }

    @Test
    public void testFirstRowComponentIsHashed() {
        long rawComponent = 1L;

        byte[] persistedRow = FooToIdIdxTable.FooToIdIdxRow.of(rawComponent).persistToBytes();

        long hashedValue = Hashing.murmur3_128().hashBytes(ValueType.FIXED_LONG.convertFromJava(rawComponent)).asLong();
        byte[] expected = PtBytes.toBytes(Long.MIN_VALUE ^ hashedValue);

        byte[] firstComponentOfRow = Arrays.copyOf(persistedRow, 8); // We're only interested in the first 8 bytes.

        Assert.assertArrayEquals(expected, firstComponentOfRow);
    }

    private IndexTestTableFactory getTableFactory() {
        return IndexTestTableFactory.of();
    }
}
