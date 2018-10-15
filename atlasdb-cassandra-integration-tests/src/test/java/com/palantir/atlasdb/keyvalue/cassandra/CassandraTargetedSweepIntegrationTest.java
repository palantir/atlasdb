/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.AbstractSweepTest;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SpecialTimestampsSupplier;
import com.palantir.atlasdb.sweep.queue.TargetedSweepFollower;
import com.palantir.atlasdb.sweep.queue.TargetedSweeper;
import com.palantir.lock.v2.TimelockService;

public class CassandraTargetedSweepIntegrationTest extends AbstractSweepTest {
    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    private SpecialTimestampsSupplier timestampsSupplier = mock(SpecialTimestampsSupplier.class);
    private TargetedSweeper sweepQueue;

    @Rule
    public final RuleChain ruleChain = SchemaMutationLockReleasingRule.createChainedReleaseAndRetry(CASSANDRA);

    public CassandraTargetedSweepIntegrationTest() {
        super(CASSANDRA, CASSANDRA);
    }

    @Before
    public void setup() {
        super.setup();

        sweepQueue = TargetedSweeper.createUninitializedForTest(() -> 1);
        sweepQueue.initializeWithoutRunning(
                timestampsSupplier, mock(TimelockService.class), kvs, mock(TargetedSweepFollower.class));
    }

    @Override
    protected Optional<SweepResults> completeSweep(TableReference tableReference, long ts) {
        when(timestampsSupplier.getUnreadableTimestamp()).thenReturn(ts);
        when(timestampsSupplier.getImmutableTimestamp()).thenReturn(ts);
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(0));
        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(0));
        return Optional.empty();
    }

    @Override
    protected void put(final TableReference tableRef, Cell cell, final String val, final long ts) {
        super.put(tableRef, cell, val, ts);
        sweepQueue.enqueue(ImmutableMap.of(tableRef, ImmutableMap.of(cell, val.getBytes(StandardCharsets.UTF_8))), ts);
    }
}
