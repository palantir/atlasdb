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

package com.palantir.atlasdb.cleaner;

import java.util.SortedMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.memory.InMemoryAtlasDbFactory;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.api.Transaction.TransactionType;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.base.BatchingVisitableView;

public class ScrubberTest {
    private Namespace ns = Namespace.create("test");
    private TableReference table = TableReference.create(ns, "table");
    private String row = "row1";
    private String col = "col1";
    private Cell cell = Cell.create(row.getBytes(), col.getBytes());
    private AtlasSchema schema = new AtlasSchema() {
        @Override
        public Namespace getNamespace() {
            return ns;
        }

        @Override
        public Schema getLatestSchema() {
            return new Schema();
        }
    };
    private SerializableTransactionManager mgr = InMemoryAtlasDbFactory.createInMemoryTransactionManager(schema);

    @Before
    public void setup() {
        mgr.getKeyValueService().createTables(ImmutableMap.of(table, AtlasDbConstants.GENERIC_TABLE_METADATA));
    }

    @Test
    public void testSimpleScrub() {
        mgr.runTaskThrowOnConflict((tx) -> {
            tx.put(table, ImmutableMap.of(cell, "val1".getBytes()));
            return null;
        });
        mgr.runTaskThrowOnConflict((tx) -> {
            tx.put(table, ImmutableMap.of(cell, "val2".getBytes()));
            tx.setTransactionType(TransactionType.HARD_DELETE);
            return null;
        });
        mgr.runTaskThrowOnConflict((tx) -> {
            tx.put(table, ImmutableMap.of(cell, "val3".getBytes()));
            tx.setTransactionType(TransactionType.HARD_DELETE);
            return null;
        });
        KeyValueService kv = mgr.getKeyValueService();
        long immutableTimestamp = mgr.getImmutableTimestamp();
        Assert.assertEquals(3, kv.getAllTimestamps(table, ImmutableSet.of(cell), immutableTimestamp).size());
        ScrubberStore scrubberStore = KeyValueServiceScrubberStore.create(kv);
        ImmutableList<SortedMap<Long, Multimap<TableReference, Cell>>> toScrub =
                BatchingVisitableView
                        .of(scrubberStore.getBatchingVisitableScrubQueue(1, immutableTimestamp, null, null))
                        .immutableCopy();

        Assert.assertEquals(1, toScrub.size());
        Assert.assertEquals(1, scrubberStore.getNumberRemainingScrubCells(100));
        SimpleCleaner cleaner = (SimpleCleaner) mgr.getCleaner();
        Scrubber scrubber = cleaner.getScrubber().withAggressiveScrub(Suppliers.ofInstance(immutableTimestamp));
        scrubber.runBackgroundScrubTask(mgr);

        Assert.assertEquals(0, scrubberStore.getNumberRemainingScrubCells(100));

        // We should have the most recent timestamp and a sentinal value.
        Multimap<Cell, Long> allTimestamps = kv.getAllTimestamps(table, ImmutableSet.of(cell), immutableTimestamp);
        Assert.assertEquals(2, allTimestamps.size());
        Assert.assertEquals(Value.INVALID_VALUE_TIMESTAMP, (long) Ordering.natural().min(allTimestamps.values()));
    }

    @Test
    public void testScrubInvalid() {
        KeyValueService kv = mgr.getKeyValueService();
        mgr.runTaskThrowOnConflict((tx) -> {
            tx.put(table, ImmutableMap.of(cell, "val1".getBytes()));
            return null;
        });
        mgr.runTaskThrowOnConflict((tx) -> {
            tx.put(table, ImmutableMap.of(cell, "val2".getBytes()));
            tx.setTransactionType(TransactionType.HARD_DELETE);
            return null;
        });
        try {
            mgr.runTaskThrowOnConflict((tx) -> {
                tx.put(table, ImmutableMap.of(cell, "val3".getBytes()));
                tx.setTransactionType(TransactionType.HARD_DELETE);
                kv.put(TransactionConstants.TRANSACTION_TABLE,
                        ImmutableMap.of(Cell.create(TransactionConstants.getValueForTimestamp(tx.getTimestamp()),
                                TransactionConstants.COMMIT_TS_COLUMN),
                                TransactionConstants.getValueForTimestamp(TransactionConstants.FAILED_COMMIT_TS)),
                        0L);
                return null;
            });
        } catch (Throwable t) {
            // We caused this on purpose to test scrubbing
        }
        long immutableTimestamp = mgr.getImmutableTimestamp();
        Multimap<Cell, Long> allTimestamps = kv.getAllTimestamps(table, ImmutableSet.of(cell), immutableTimestamp);
        Assert.assertEquals(3, allTimestamps.size());
        ScrubberStore scrubberStore = KeyValueServiceScrubberStore.create(kv);
        ImmutableList<SortedMap<Long, Multimap<TableReference, Cell>>> toScrub =
                BatchingVisitableView
                        .of(scrubberStore.getBatchingVisitableScrubQueue(1, immutableTimestamp, null, null))
                        .immutableCopy();

        Assert.assertEquals(1, toScrub.size());
        Assert.assertEquals(1, scrubberStore.getNumberRemainingScrubCells(100));
        SimpleCleaner cleaner = (SimpleCleaner) mgr.getCleaner();
        Scrubber scrubber = cleaner.getScrubber().withAggressiveScrub(Suppliers.ofInstance(immutableTimestamp));
        scrubber.runBackgroundScrubTask(mgr);

        Assert.assertEquals(1, scrubberStore.getNumberRemainingScrubCells(100));

        // We should have the most recent 2 values and no sentinal value.
        allTimestamps = kv.getAllTimestamps(table, ImmutableSet.of(cell), immutableTimestamp);
        Assert.assertEquals(2, allTimestamps.size());
        Assert.assertNotEquals(Value.INVALID_VALUE_TIMESTAMP, (long) Ordering.natural().min(allTimestamps.values()));

        scrubber.runBackgroundScrubTask(mgr);

        // Now we should be in a good state.
        allTimestamps = kv.getAllTimestamps(table, ImmutableSet.of(cell), immutableTimestamp);
        Assert.assertEquals(2, allTimestamps.size());
        Assert.assertEquals(Value.INVALID_VALUE_TIMESTAMP, (long) Ordering.natural().min(allTimestamps.values()));
    }

}
