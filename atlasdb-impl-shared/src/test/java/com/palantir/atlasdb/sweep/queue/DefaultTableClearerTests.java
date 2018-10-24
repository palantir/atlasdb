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

package com.palantir.atlasdb.sweep.queue;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public final class DefaultTableClearerTests {
    private static final TableReference TABLE = TableReference.create(Namespace.create("foo"), "bar");
    private static final Cell CELL = Cell.create(new byte[1], new byte[2]);
    private final KeyValueService kvs = new InMemoryKeyValueService(false);
    private long immutableTimestamp = 0;

    private DefaultTableClearer clearer = new DefaultTableClearer(kvs, () -> immutableTimestamp);

    @Before
    public void before() {
        Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
    }

    @Test
    public void testFilter_ignoresIfThorough() {
        createTable(SweepStrategy.THOROUGH);
        clearer.updateWatermarksForTables(2, ImmutableSet.of(TABLE));
        WriteInfo write = writeInfo(1);
        assertThat(clearer.filter(ImmutableList.of(write))).containsExactly(write);
    }

    @Test
    public void testFilter_ignoresIfNoSweep() {
        createTable(SweepStrategy.NOTHING);
        clearer.updateWatermarksForTables(2, ImmutableSet.of(TABLE));
        WriteInfo write = writeInfo(1);
        assertThat(clearer.filter(ImmutableList.of(write))).containsExactly(write);
    }

    @Test
    public void testFilter_filtersIfConservativeSweepAndBehindWatermark() {
        createTable(SweepStrategy.CONSERVATIVE);
        clearer.updateWatermarksForTables(2, ImmutableSet.of(TABLE));
        WriteInfo write = writeInfo(1);
        assertThat(clearer.filter(ImmutableList.of(write))).isEmpty();
    }

    @Test
    public void testFilter_doesNotFilterIfAheadOfWatermark() {
        createTable(SweepStrategy.CONSERVATIVE);
        clearer.updateWatermarksForTables(2, ImmutableSet.of(TABLE));
        WriteInfo write = writeInfo(3);
        assertThat(clearer.filter(ImmutableList.of(write))).containsExactly(write);
    }

    @Test
    public void testWatermarkUpdates_newWatermark() {
        clearer.updateWatermarksForTables(2, ImmutableSet.of(TABLE));
        assertThat(clearer.getWatermarks(ImmutableSet.of(TABLE))).containsEntry(TABLE, 2L);
    }

    @Test
    public void testWatermarkUpdates_laterWatermarkUnchanged() {
        clearer.updateWatermarksForTables(4, ImmutableSet.of(TABLE));
        clearer.updateWatermarksForTables(3, ImmutableSet.of(TABLE));
        assertThat(clearer.getWatermarks(ImmutableSet.of(TABLE))).containsEntry(TABLE, 4L);
    }

    @Test
    public void testDestructiveAction_watermarkUpdateIfConservative() {
        createTable(SweepStrategy.CONSERVATIVE);
        immutableTimestamp = 4;
        assertThat(clearer.getWatermarks(ImmutableSet.of(TABLE))).isEmpty();
        clearer.truncateTables(ImmutableSet.of(TABLE));
        assertThat(clearer.getWatermarks(ImmutableSet.of(TABLE))).containsEntry(TABLE, 4L);
    }

    @Test
    public void testDestructiveAction_noWatermarkUpdateIfThorough() {
        createTable(SweepStrategy.THOROUGH);
        immutableTimestamp = 4;
        clearer.truncateTables(ImmutableSet.of(TABLE));
        assertThat(clearer.getWatermarks(ImmutableSet.of(TABLE))).isEmpty();
    }

    private static WriteInfo writeInfo(long timestamp) {
        return WriteInfo.write(TABLE, CELL, timestamp);
    }

    private void createTable(SweepStrategy sweepStrategy) {
        TableMetadata metadata = new TableMetadata(
                new NameMetadataDescription(),
                new ColumnMetadataDescription(),
                ConflictHandler.RETRY_ON_WRITE_WRITE,
                TableMetadataPersistence.CachePriority.COLD,
                true,
                1,
                false,
                sweepStrategy,
                false);
        kvs.createTable(TABLE, metadata.persistToBytes());
    }
}
