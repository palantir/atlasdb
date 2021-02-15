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

package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.keyvalue.impl.TransactionManagerManager;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SpecialTimestampsSupplier;
import com.palantir.atlasdb.sweep.queue.TargetedSweepFollower;
import com.palantir.atlasdb.sweep.queue.TargetedSweeper;
import com.palantir.atlasdb.sweep.queue.config.ImmutableTargetedSweepRuntimeConfig;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.v2.TimelockService;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class AbstractTargetedSweepTest extends AbstractSweepTest {
    private static final TableReference TABLE_TO_BE_DROPPED = TableReference.createFromFullyQualifiedName("ts.drop");
    private static final Cell TEST_CELL = Cell.create(PtBytes.toBytes("r"), PtBytes.toBytes("c"));
    private static final String OLD_VALUE = "old_value";
    private static final String NEW_VALUE = "new_value";
    private SpecialTimestampsSupplier timestampsSupplier = mock(SpecialTimestampsSupplier.class);
    private TargetedSweeper sweepQueue;

    protected AbstractTargetedSweepTest(KvsManager kvsManager, TransactionManagerManager tmManager) {
        super(kvsManager, tmManager);
    }

    @Before
    @Override
    public void setup() {
        super.setup();

        MetricsManager metricsManager = MetricsManagers.createForTests();
        sweepQueue = TargetedSweeper.createUninitializedForTest(
                metricsManager, () -> ImmutableTargetedSweepRuntimeConfig.builder()
                        .shards(1)
                        .maximumPartitionsToBatchInSingleRead(8)
                        .build());
        sweepQueue.initializeWithoutRunning(
                timestampsSupplier, mock(TimelockService.class), kvs, txService, mock(TargetedSweepFollower.class));
    }

    @Override
    protected Optional<SweepResults> completeSweep(TableReference ignored, long ts) {
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(0), ts);
        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(0), ts);
        return Optional.empty();
    }

    @Override
    protected void put(final TableReference tableRef, Cell cell, final String val, final long ts) {
        super.put(tableRef, cell, val, ts);
        sweepQueue.enqueue(ImmutableMap.of(tableRef, ImmutableMap.of(cell, PtBytes.toBytes(val))), ts);
    }

    @Test
    public void targetedSweepIgnoresDroppedTables() {
        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
        kvs.createTable(TABLE_TO_BE_DROPPED, TableMetadata.allDefault().persistToBytes());

        put(TABLE_NAME, TEST_CELL, OLD_VALUE, 50);
        put(TABLE_NAME, TEST_CELL, NEW_VALUE, 150);
        put(TABLE_TO_BE_DROPPED, TEST_CELL, OLD_VALUE, 100);

        kvs.dropTable(TABLE_TO_BE_DROPPED);
        completeSweep(null, 90);
        assertThat(getValue(TABLE_NAME, 110)).isEqualTo(Value.create(PtBytes.toBytes(OLD_VALUE), 50));
        assertThat(getValue(TABLE_NAME, 160)).isEqualTo(Value.create(PtBytes.toBytes(NEW_VALUE), 150));

        completeSweep(null, 160);
        assertThat(getValue(TABLE_NAME, 110))
                .isEqualTo(Value.create(PtBytes.EMPTY_BYTE_ARRAY, Value.INVALID_VALUE_TIMESTAMP));
        assertThat(getValue(TABLE_NAME, 160)).isEqualTo(Value.create(PtBytes.toBytes(NEW_VALUE), 150));
    }

    @Test
    public void targetedSweepIgnoresDroppedTablesForUncommittedWrites() {
        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
        kvs.createTable(TABLE_TO_BE_DROPPED, TableMetadata.allDefault().persistToBytes());

        sweepQueue.enqueue(
                ImmutableMap.of(TABLE_TO_BE_DROPPED, ImmutableMap.of(TEST_CELL, PtBytes.toBytes(OLD_VALUE))), 100);

        kvs.dropTable(TABLE_TO_BE_DROPPED);

        completeSweep(null, 160);
    }

    private Value getValue(TableReference tableRef, long ts) {
        return kvs.get(tableRef, ImmutableMap.of(TEST_CELL, ts)).get(TEST_CELL);
    }
}
