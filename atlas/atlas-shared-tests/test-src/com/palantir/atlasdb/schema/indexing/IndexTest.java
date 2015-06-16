// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.schema.indexing;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbTestCase;
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
import com.palantir.atlasdb.transaction.api.RuntimeTransactionTask;
import com.palantir.atlasdb.transaction.api.Transaction;

public class IndexTest extends AtlasDbTestCase {

    @Before
    public void createSchema() {
        IndexTestSchema.getSchema().deleteTablesAndIndexes(keyValueService);
        IndexTestSchema.getSchema().createTablesAndIndexes(keyValueService);
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
                DataTable.Index1IdxTable index1 = DataTable.Index1IdxTable.of(t);
                DataTable.Index2IdxTable index2 = DataTable.Index2IdxTable.of(t);
                DataTable.Index3IdxTable index3 = DataTable.Index3IdxTable.of(t);
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
                DataTable.Index1IdxTable index1 = DataTable.Index1IdxTable.of(t);
                DataTable.Index2IdxTable index2 = DataTable.Index2IdxTable.of(t);
                DataTable.Index3IdxTable index3 = DataTable.Index3IdxTable.of(t);
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
                DataTable.Index1IdxTable index1 = DataTable.Index1IdxTable.of(t);
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
                DataTable.Index1IdxTable index1 = DataTable.Index1IdxTable.of(t);
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
                FooToIdIdxTable index = FooToIdIdxTable.of(t);
                List<FooToIdIdxRowResult> result = index.getAllRowsUnordered().immutableCopy();
                Assert.assertEquals(2L, Iterables.getOnlyElement(result).getRowName().getFoo());
                return null;
            }
        });
    }

    private IndexTestTableFactory getTableFactory() {
        return IndexTestTableFactory.of();
    }
}
