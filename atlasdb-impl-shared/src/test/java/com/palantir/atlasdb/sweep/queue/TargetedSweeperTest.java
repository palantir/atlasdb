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

import static com.palantir.atlasdb.sweep.metrics.SweepMetricsAssert.assertThat;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.BATCH_SIZE_KVS;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.MAX_CELLS_GENERIC;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.SWEEP_BATCH_SIZE;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.TS_COARSE_GRANULARITY;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.TS_FINE_GRANULARITY;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.maxTsForFinePartition;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.minTsForCoarsePartition;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.minTsForFinePartition;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.tsPartitionFine;
import static com.palantir.atlasdb.table.description.SweeperStrategy.CONSERVATIVE;
import static com.palantir.atlasdb.table.description.SweeperStrategy.THOROUGH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.math.IntMath;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.KeyValueServicePuncherStore;
import com.palantir.atlasdb.cleaner.PuncherStore;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.metrics.ImmutableMetricsConfiguration;
import com.palantir.atlasdb.sweep.metrics.SweepOutcome;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetricsConfigurations;
import com.palantir.atlasdb.sweep.queue.config.ImmutableTargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.config.ImmutableTargetedSweepRuntimeConfig;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepRuntimeConfig;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.base.ClosableIterator;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("MustBeClosedChecker")
public class TargetedSweeperTest extends AbstractSweepQueueTest {
    private static final String PARAMETERIZED_TEST_NAME = "{index}: input=\"{0}\"";

    public static List<Integer> readBatchSizes() {
        // Tests have an assumption that the read batch size is less than half of the number of coarse
        // partitions (SweepQueueUtils.TS_COARSE_GRANULARITY / SweepQueueUtils.TS_FINE_GRANULARITY / 2).
        return List.of(1, 8, 99);
    }

    private static final long LOW_TS = 10L;
    private static final long LOW_TS2 = 2 * LOW_TS;
    private static final long LOW_TS3 = 3 * LOW_TS;
    private static final long SMALL_REFRESH_MILLIS = 10L;

    private TargetedSweeper targetedSweeper;
    private MultiTableSweepQueueWriter sweepQueue;
    private ShardProgress progress;
    private SweepableTimestamps sweepableTimestamps;
    private SweepableCells sweepableCells;
    private TargetedSweepFollower mockFollower;
    private TimelockService timelockService;
    private PuncherStore puncherStore;
    private SingleBatchSweeper sweeper;
    private final SettableRefreshable<TargetedSweepRuntimeConfig> runtimeConfig =
            Refreshable.create(ImmutableTargetedSweepRuntimeConfig.builder()
                    .enabled(true)
                    .enableAutoTuning(false)
                    .shards(DEFAULT_SHARDS)
                    .build());
    private final TargetedSweepInstallConfig installConfig = ImmutableTargetedSweepInstallConfig.builder()
            .conservativeThreads(0)
            .thoroughThreads(0)
            .metricsConfiguration(ImmutableMetricsConfiguration.builder()
                    .from(TargetedSweepMetricsConfigurations.DEFAULT)
                    .millisBetweenRecomputingMetrics(SMALL_REFRESH_MILLIS)
                    .lastSweptTimestampUpdaterDelayMillis(SMALL_REFRESH_MILLIS)
                    .build())
            .build();

    @AfterEach
    @Override
    public void tearDown() {
        // This is required because of JUnit memory issues
        targetedSweeper.close();
        targetedSweeper = null;
        sweeper = null;
        sweepQueue = null;
        progress = null;
        sweepableTimestamps = null;
        sweepableCells = null;
        puncherStore = null;
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void secondaryQueueReadsFirstQueueLastSweptTimestamp(int readBatchSize) {
        setup(readBatchSize);
        MetricsManager secondQueueManager = MetricsManagers.createForTests();
        TargetedSweeper secondQueue = TargetedSweeper.createUninitialized(
                secondQueueManager,
                runtimeConfig,
                installConfig,
                ImmutableList.of(),
                _unused -> {},
                spiedKvs,
                SettableFuture.create(),
                puncherStore);
        secondQueue.initializeWithoutRunning(
                timestampsSupplier,
                timelockService,
                spiedKvs,
                TransactionServices.createV1TransactionService(spiedKvs),
                mockFollower);
        secondQueue.runInBackground();
        await(() -> assertThat(secondQueueManager).hasLastSweptTimestampConservativeEqualTo(-1L));

        enqueueWriteCommitted(TABLE_CONS, LOW_TS);
        sweepNextBatchForShards(CONSERVATIVE, DEFAULT_SHARDS);
        await(() -> assertThat(secondQueueManager).hasLastSweptTimestampConservativeEqualTo(maxTsForFinePartition(0)));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void increaseInShardsReflectedOnLastSweptTimestamp(int readBatchSize) {
        setup(readBatchSize);
        targetedSweeper.runInBackground();
        enqueueWriteCommitted(TABLE_CONS, LOW_TS);
        sweepNextBatchForShards(CONSERVATIVE, DEFAULT_SHARDS);
        await(() -> assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(maxTsForFinePartition(0)));

        runtimeConfig.update(ImmutableTargetedSweepRuntimeConfig.builder()
                .from(runtimeConfig.get())
                .shards(DEFAULT_SHARDS + 1)
                .build());

        // Shard count is memoized, creating a second queue bypasses waiting for expiration
        TargetedSweeper secondQueue = TargetedSweeper.createUninitialized(
                metricsManager,
                runtimeConfig,
                installConfig,
                ImmutableList.of(),
                _unused -> {},
                spiedKvs,
                SettableFuture.create(),
                puncherStore);
        secondQueue.initializeWithoutRunning(
                timestampsSupplier,
                mock(TimelockService.class),
                spiedKvs,
                TransactionServices.createV1TransactionService(spiedKvs),
                mock(TargetedSweepFollower.class));
        secondQueue.runInBackground();
        await(() -> assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(-1L));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void initializingWithUninitializedKvsThrows(int readBatchSize) {
        setup(readBatchSize);
        KeyValueService uninitializedKvs = mock(KeyValueService.class);
        when(uninitializedKvs.isInitialized()).thenReturn(false);
        TargetedSweeper sweeper =
                TargetedSweeper.createUninitializedForTest(spiedKvs, Refreshable.only(1), SettableFuture.create());
        assertThatThrownBy(() -> sweeper.initializeWithoutRunning(
                        null,
                        mock(TimelockService.class),
                        uninitializedKvs,
                        txnService,
                        mock(TargetedSweepFollower.class)))
                .isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void enqueueUpdatesNumberOfShards(int readBatchSize) {
        setup(readBatchSize);
        assertThat(AtlasDbConstants.LEGACY_DEFAULT_TARGETED_SWEEP_SHARDS).isLessThan(DEFAULT_SHARDS);
        assertThat(progress.getNumberOfShards()).isEqualTo(AtlasDbConstants.LEGACY_DEFAULT_TARGETED_SWEEP_SHARDS);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS);
        assertThat(progress.getNumberOfShards()).isEqualTo(DEFAULT_SHARDS);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepStrategyNothingDoesNotPersistAnything(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_NOTH, LOW_TS);
        enqueueWriteCommitted(TABLE_NOTH, LOW_TS2);
        verify(spiedKvs, times(2)).put(eq(TABLE_NOTH), anyMap(), anyLong());
        verify(spiedKvs, times(2)).put(any(TableReference.class), anyMap(), anyLong());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepStrategyNothingDoesNotUpdateMetrics(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_NOTH, LOW_TS);
        enqueueWriteCommitted(TABLE_NOTH, LOW_TS2);
        assertThat(metricsManager).hasEnqueuedWritesConservativeEqualTo(0);
        assertThat(metricsManager).hasEnqueuedWritesThoroughEqualTo(0);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void conservativeSweepAddsSentinelAndLeavesSingleValue(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS);
        assertReadAtTimestampReturnsNothing(TABLE_CONS, LOW_TS);

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepsThoroughMigrationAsConservative(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_THOR_MIGRATION, LOW_TS);
        assertReadAtTimestampReturnsNothing(TABLE_THOR_MIGRATION, LOW_TS);

        sweepNextBatch(ShardAndStrategy.conservative(THOR_MIGRATION_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_THOR_MIGRATION, LOW_TS);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_THOR_MIGRATION, LOW_TS);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepWithSingleEntryUpdatesMetrics(int readBatchSize) {
        setup(readBatchSize);
        targetedSweeper.runInBackground();
        enqueueWriteCommitted(TABLE_CONS, LOW_TS);
        sweepNextBatchForShards(CONSERVATIVE, DEFAULT_SHARDS);

        assertThat(metricsManager).hasTombstonesPutConservativeEqualTo(1);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(getSweepTsCons());

        await(() -> assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(maxTsForFinePartition(0)));

        setTimelockTime(5_000L);
        punchTimeAtTimestamp(2_000, LOW_TS);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(5_000L - 2_000L);
        assertThat(metricsManager).hasTargetedOutcomeEqualTo(CONSERVATIVE, SweepOutcome.SUCCESS, 1L);
        assertThat(metricsManager).hasNotRegisteredTargetedOutcome(THOROUGH, SweepOutcome.SUCCESS);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepWithNoCandidatesBeforeSweepTimestampReportsNothingToSweep(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_CONS, getSweepTsCons());
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));

        assertThat(metricsManager).hasTargetedOutcomeEqualTo(CONSERVATIVE, SweepOutcome.NOTHING_TO_SWEEP, 1L);
        assertThat(metricsManager).hasNotRegisteredTargetedOutcome(THOROUGH, SweepOutcome.NOTHING_TO_SWEEP);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void thoroughSweepDoesNotAddSentinelAndLeavesSingleValue(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_THOR, LOW_TS);
        assertReadAtTimestampReturnsNothing(TABLE_THOR, LOW_TS);

        sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOR, LOW_TS);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_THOR, LOW_TS);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void thoroughSweepDeletesExistingSentinel(int readBatchSize) {
        setup(readBatchSize);
        spiedKvs.addGarbageCollectionSentinelValues(TABLE_THOR, ImmutableList.of(DEFAULT_CELL));
        assertReadAtTimestampReturnsSentinel(TABLE_THOR, 0L);
        enqueueWriteCommitted(TABLE_THOR, 10L);
        sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOR, 0L);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void conservativeSweepDeletesLowerValue(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS2);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS2);

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS2);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void thoroughSweepDeletesLowerValue(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_THOR, LOW_TS);
        enqueueWriteCommitted(TABLE_THOR, LOW_TS2);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_THOR, LOW_TS);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_THOR, LOW_TS2);

        sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOR, LOW_TS + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_THOR, LOW_TS2);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void conservativeSweepCallsFollower(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS2);
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));

        ArgumentCaptor<Set<Cell>> captor = ArgumentCaptor.forClass(Set.class);
        verify(mockFollower, times(1)).run(eq(TABLE_CONS), captor.capture());
        assertThat(Iterables.getOnlyElement(captor.getAllValues())).containsExactly(DEFAULT_CELL);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void thoroughSweepCallsFollower(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_THOR, LOW_TS);
        enqueueWriteCommitted(TABLE_THOR, LOW_TS2);
        sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));

        ArgumentCaptor<Set<Cell>> captor = ArgumentCaptor.forClass(Set.class);
        verify(mockFollower, times(1)).run(eq(TABLE_THOR), captor.capture());
        assertThat(Iterables.getOnlyElement(captor.getAllValues())).containsExactly(DEFAULT_CELL);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void conservativeSweepDeletesAllButLatestWithSingleDeleteAllTimestamps(int readBatchSize) {
        setup(readBatchSize);
        long lastWriteTs = 5000;
        for (long i = 1; i <= lastWriteTs; i++) {
            enqueueWriteCommitted(TABLE_CONS, i);
        }
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, lastWriteTs);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, lastWriteTs);
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void thoroughSweepDeletesAllButLatestWithSingleDeleteAllTimestampsIncludingSentinels(int readBatchSize) {
        setup(readBatchSize);
        long lastWriteTs = 5000;
        for (long i = 1; i <= lastWriteTs; i++) {
            enqueueWriteCommitted(TABLE_THOR, i);
        }
        sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOR, lastWriteTs);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_THOR, lastWriteTs);
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepsOnlyThePrescribedNumberOfBatchesAtATime(int readBatchSize) {
        setup(readBatchSize);
        for (int partition = 0; partition <= readBatchSize; partition++) {
            enqueueWriteCommitted(TABLE_CONS, LOW_TS + SweepQueueUtils.minTsForFinePartition(partition));
            enqueueWriteCommitted(TABLE_CONS, LOW_TS + SweepQueueUtils.minTsForFinePartition(partition) + 1);
        }

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(
                TABLE_CONS, LOW_TS + SweepQueueUtils.minTsForFinePartition(readBatchSize - 1));
        assertTestValueEnqueuedAtGivenTimestampStillPresent(
                TABLE_CONS, LOW_TS + SweepQueueUtils.minTsForFinePartition(readBatchSize - 1) + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(
                TABLE_CONS, LOW_TS + SweepQueueUtils.minTsForFinePartition(readBatchSize));

        assertThat(metricsManager).containsEntriesReadInBatchConservative(readBatchSize * 2);
        assertThat(metricsManager).containsEntriesReadInBatchThorough();
        assertThat(metricsManager).hasEntriesReadInBatchMeanConservativeEqualTo(readBatchSize * 2);
        assertThat(metricsManager).hasEntriesReadInBatchMeanThoroughEqualTo(0.0);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepDeletesWritesWhenTombstoneHasHigherTimestamp(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS);
        enqueueTombstone(TABLE_CONS, LOW_TS2);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_CONS, LOW_TS2 + 1, LOW_TS2);

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS + 1);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_CONS, LOW_TS2 + 1, LOW_TS2);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void thoroughSweepDeletesTombstoneIfLatestWrite(int readBatchSize) {
        setup(readBatchSize);
        enqueueTombstone(TABLE_THOR, LOW_TS);
        enqueueTombstone(TABLE_THOR, LOW_TS2);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_THOR, LOW_TS + 1, LOW_TS);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_THOR, LOW_TS2 + 1, LOW_TS2);

        sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOR, LOW_TS + 1);
        assertReadAtTimestampReturnsNothing(TABLE_THOR, LOW_TS2 + 1);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepDeletesTombstonesWhenWriteHasHigherTimestamp(int readBatchSize) {
        setup(readBatchSize);
        enqueueTombstone(TABLE_CONS, LOW_TS);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS2);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_CONS, LOW_TS + 1, LOW_TS);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS2);

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS2);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepHandlesSequencesOfDeletesAndReadditionsInOneShot(int readBatchSize) {
        setup(readBatchSize);
        targetedSweeper.runInBackground();
        enqueueWriteCommitted(TABLE_CONS, LOW_TS);
        enqueueTombstone(TABLE_CONS, LOW_TS + 2);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS + 4);
        enqueueTombstone(TABLE_CONS, LOW_TS + 6);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS + 8);
        // ensure not all entries will be swept
        for (int partition = 0; partition < readBatchSize; partition++) {
            enqueueWriteCommitted(TABLE_CONS, minTsForFinePartition(partition + 1));
        }

        sweepNextBatchForShards(CONSERVATIVE, DEFAULT_SHARDS);
        for (int i = 0; i < 10; i = i + 2) {
            assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS + i);
        }
        if (readBatchSize == 1) {
            assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS + 8);
        } else {
            assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, minTsForFinePartition(readBatchSize));
        }
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(), any());

        assertThat(metricsManager).hasTombstonesPutConservativeEqualTo(1);

        await(() -> assertThat(metricsManager)
                .hasLastSweptTimestampConservativeEqualTo(maxTsForFinePartition(readBatchSize - 1)));

        setTimelockTime(10_000L);
        punchTimeAtTimestamp(5_000L, LOW_TS + 8);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(10_000L - 5000L);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void enableAutoTuningOverridesEffectivelySetsLargeReadBatchSize(int readBatchSize) {
        setup(readBatchSize);
        runtimeConfig.update(ImmutableTargetedSweepRuntimeConfig.builder()
                .from(runtimeConfig.get())
                .enableAutoTuning(true)
                .build());

        for (int partition = 0; partition < readBatchSize * 5; partition++) {
            enqueueWriteCommitted(TABLE_CONS, minTsForFinePartition(partition + 1));
        }

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));

        for (int partition = 0; partition < readBatchSize * 5 - 1; partition++) {
            assertReadAtTimestampReturnsSentinel(TABLE_CONS, minTsForFinePartition(partition + 1));
        }
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, minTsForFinePartition(readBatchSize * 5));
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(), any());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepProgressesAndSkipsEmptyFinePartitions(int readBatchSize) {
        setup(readBatchSize);
        targetedSweeper.runInBackground();
        setSweepTimestamp(minTsForFinePartition(2 * (2 * readBatchSize) + 2));
        List<Integer> permittedPartitions = new ArrayList<>();
        for (int index = 0; index <= 2 * readBatchSize; index++) {
            int partitionToUse = 2 * index;
            long timestampToUse = minTsForFinePartition(partitionToUse) + LOW_TS;
            enqueueWriteCommitted(TABLE_CONS, timestampToUse);
            punchTimeAtTimestamp(100 * (index + 1), timestampToUse);
            permittedPartitions.add(partitionToUse);
        }

        int finalPartition = 2 * (2 * readBatchSize);
        long additionalTimestampForFinalPartition = minTsForFinePartition(finalPartition) + LOW_TS + 1;
        enqueueWriteCommitted(TABLE_CONS, additionalTimestampForFinalPartition);
        int finalValueWallClockTime = 100 * (2 * readBatchSize + 1) + 1;
        punchTimeAtTimestamp(finalValueWallClockTime, additionalTimestampForFinalPartition);

        // additional unsweepable value
        enqueueWriteCommitted(TABLE_CONS, minTsForFinePartition(9_999_999));
        punchTimeAtTimestamp(Long.MAX_VALUE, minTsForFinePartition(9_999_999));

        // first sweep writes a sentinel for the last partition in the batch, but doesn't clear it
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        long timestampEndOfBatchOne = minTsForFinePartition(permittedPartitions.get(readBatchSize - 1)) + LOW_TS;
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, timestampEndOfBatchOne);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, timestampEndOfBatchOne);
        assertThat(metricsManager).hasTombstonesPutConservativeEqualTo(1);

        // second sweep writes a sentinel for the last partition in the second batch, but doesn't clear it
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        long timestampEndOfBatchTwo = minTsForFinePartition(permittedPartitions.get(2 * readBatchSize - 1)) + LOW_TS;
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, timestampEndOfBatchTwo);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, timestampEndOfBatchTwo);
        assertThat(metricsManager).hasTombstonesPutConservativeEqualTo(2);

        // third sweep deletes all but last sweepable entry
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        long lastSweepableTimestamp = minTsForFinePartition(permittedPartitions.get(2 * readBatchSize)) + LOW_TS + 1;
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, lastSweepableTimestamp);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, lastSweepableTimestamp);
        assertThat(metricsManager).hasTombstonesPutConservativeEqualTo(3);
        assertThat(metricsManager).hasEntriesReadConservativeEqualTo(2 * readBatchSize + 2);
        assertThat(progress.getLastSweptTimestamp(ShardAndStrategy.conservative(CONS_SHARD)))
                .isEqualTo(maxTsForFinePartition(permittedPartitions.get(2 * readBatchSize)));
        assertThat(metricsManager).containsEntriesReadInBatchConservative(readBatchSize, readBatchSize, 2L);
        assertThat(metricsManager).hasEntriesReadInBatchMeanConservativeEqualTo((2 * readBatchSize + 2) / 3.0);

        sweepNextBatchForShards(CONSERVATIVE, DEFAULT_SHARDS);
        setTimelockTime(finalValueWallClockTime * 3);

        await(() ->
                assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(finalValueWallClockTime * 2L));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepProgressesAcrossCoarsePartitions(int readBatchSize) {
        setup(readBatchSize);
        setSweepTimestamp(Long.MAX_VALUE);
        List<Integer> permittedPartitions = new ArrayList<>();
        permittedPartitions.add(0);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS);

        for (int index = 0; index <= 3 * readBatchSize; index++) {
            int partitionToUse = 3 * index + 1;
            enqueueWriteCommitted(TABLE_CONS, minTsForCoarsePartition(partitionToUse) + LOW_TS);
            permittedPartitions.add(partitionToUse);
        }

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(
                TABLE_CONS, LOW_TS + minTsForCoarsePartition(permittedPartitions.get(readBatchSize - 1)));
        assertTestValueEnqueuedAtGivenTimestampStillPresent(
                TABLE_CONS, LOW_TS + minTsForCoarsePartition(permittedPartitions.get(readBatchSize)));

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(
                TABLE_CONS, LOW_TS + minTsForCoarsePartition(permittedPartitions.get(2 * readBatchSize - 1)));
        assertTestValueEnqueuedAtGivenTimestampStillPresent(
                TABLE_CONS, LOW_TS + minTsForCoarsePartition(permittedPartitions.get(2 * readBatchSize)));

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(
                TABLE_CONS, LOW_TS + minTsForCoarsePartition(permittedPartitions.get(3 * readBatchSize - 1)));
        assertTestValueEnqueuedAtGivenTimestampStillPresent(
                TABLE_CONS, LOW_TS + minTsForCoarsePartition(permittedPartitions.get(3 * readBatchSize)));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepProgressesToJustBeforeSweepTsWhenNothingToSweep(int readBatchSize) {
        setup(readBatchSize);
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertProgressUpdatedToTimestamp(getSweepTsCons() - 1L);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepProgressesToEndOfPartitionWhenFewValuesAndSweepTsLarge(int readBatchSize) {
        setup(readBatchSize);
        long writeTs = getSweepTsCons() - 3 * TS_FINE_GRANULARITY;
        enqueueWriteCommitted(TABLE_CONS, writeTs);
        enqueueWriteCommitted(TABLE_CONS, writeTs + 5);

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertProgressUpdatedToTimestamp(maxTsForFinePartition(tsPartitionFine(writeTs)));

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertProgressUpdatedToTimestamp(getSweepTsCons() - 1L);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepCellOnlyOnceWhenInLastPartitionBeforeSweepTs(int readBatchSize) {
        setup(readBatchSize);
        immutableTs = 2 * TS_COARSE_GRANULARITY - TS_FINE_GRANULARITY;
        verify(spiedKvs, never()).deleteAllTimestamps(any(TableReference.class), anyMap());

        enqueueWriteCommitted(TABLE_CONS, immutableTs - 1);
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepableTimestampsGetsScrubbedWhenNoMoreToSweepButSweepTsInNewCoarsePartition(int readBatchSize) {
        setup(readBatchSize);
        long largestFirstPartitionCoarse = TS_COARSE_GRANULARITY - 1L;
        for (int i = 0; i < 2 * readBatchSize; i++) {
            enqueueWriteCommitted(TABLE_CONS, LOW_TS + TS_FINE_GRANULARITY * i);
        }
        enqueueWriteCommitted(TABLE_CONS, largestFirstPartitionCoarse);

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(LOW_TS));
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(LOW_TS));

        // after this sweep we progress to sweepTsConservative - 1
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertNoEntriesInSweepableTimestampsBeforeSweepTimestamp();
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepableTimestampsGetsScrubbedWhenLastSweptProgressesInNewCoarsePartition(int readBatchSize) {
        setup(readBatchSize);
        for (int i = 0; i < 2 * readBatchSize; i++) {
            enqueueWriteCommitted(TABLE_CONS, LOW_TS + TS_FINE_GRANULARITY * i);
        }
        enqueueWriteCommitted(TABLE_CONS, TS_COARSE_GRANULARITY - 1L);
        enqueueWriteCommitted(TABLE_CONS, 2 * TS_COARSE_GRANULARITY);

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(LOW_TS));
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(LOW_TS));

        // after this sweep we progress to thirdPartitionCoarse - 1
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(2 * TS_COARSE_GRANULARITY));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepableCellsGetsScrubbedWheneverPartitionIsCompletelySwept(int readBatchSize) {
        setup(readBatchSize);
        for (int i = 0; i < readBatchSize; i++) {
            long referenceTimestamp = LOW_TS + SweepQueueUtils.minTsForFinePartition(i);
            enqueueWriteCommitted(TABLE_CONS, referenceTimestamp);
            enqueueWriteCommitted(TABLE_CONS, referenceTimestamp + 1L);
            enqueueAtLeastThresholdWritesInDefaultShardWithStartTs(100, referenceTimestamp + 2L);
            putTimestampIntoTransactionTable(referenceTimestamp + 2L, referenceTimestamp + 2L);
        }

        for (int i = readBatchSize; i < 2 * readBatchSize; i++) {
            enqueueWriteCommitted(TABLE_CONS, SweepQueueUtils.minTsForFinePartition(i));
        }
        enqueueWriteCommitted(TABLE_CONS, getSweepTsCons());

        // last swept timestamp: TS_FINE_GRANULARITY - 1: fine partitions 0 through rBS - 1 are completely swept
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSweepableCellsHasNoEntriesInPartitionOfTimestamp(LOW_TS + 1);
        assertSweepableCellsHasEntryForTimestamp(SweepQueueUtils.minTsForFinePartition(readBatchSize));
        assertSweepableCellsHasEntryForTimestamp(getSweepTsCons());
        assertSweepableCellsHasNoDedicatedRowsForShard(CONS_SHARD);

        // last swept timestamp: 2 * TS_FINE_GRANULARITY - 1: fine partitions rBS through 2*rBS - 1 are completely swept
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSweepableCellsHasNoEntriesInPartitionOfTimestamp(LOW_TS + 1);
        assertSweepableCellsHasNoEntriesInPartitionOfTimestamp(SweepQueueUtils.minTsForFinePartition(readBatchSize));
        assertSweepableCellsHasEntryForTimestamp(getSweepTsCons());
        assertSweepableCellsHasNoDedicatedRowsForShard(CONS_SHARD);

        // last swept timestamp: largestBeforeSweepTs
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSweepableCellsHasNoEntriesInPartitionOfTimestamp(LOW_TS + 1);
        assertSweepableCellsHasNoEntriesInPartitionOfTimestamp(SweepQueueUtils.minTsForFinePartition(readBatchSize));
        assertSweepableCellsHasEntryForTimestamp(getSweepTsCons());
        assertSweepableCellsHasNoDedicatedRowsForShard(CONS_SHARD);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void doesNotSweepBeyondSweepTimestamp(int readBatchSize) {
        setup(readBatchSize);
        writeValuesAroundSweepTimestampAndSweepAndCheck(getSweepTsCons(), 1);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void doesNotTransitivelyRetainWritesFromBeforeSweepTimestamp(int readBatchSize) {
        setup(readBatchSize);
        targetedSweeper.runInBackground();
        long sweepTimestamp = getSweepTsCons();
        enqueueWriteCommitted(TABLE_CONS, sweepTimestamp - 10);
        enqueueTombstone(TABLE_CONS, sweepTimestamp - 5);
        enqueueWriteCommitted(TABLE_CONS, sweepTimestamp + 5);
        sweepNextBatchForShards(CONSERVATIVE, DEFAULT_SHARDS);
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, sweepTimestamp - 5);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_CONS, sweepTimestamp - 5 + 1, sweepTimestamp - 5);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, sweepTimestamp + 5);
        assertThat(metricsManager).hasTombstonesPutConservativeEqualTo(1);

        await(() -> assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(sweepTimestamp - 1));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepsOnlySweepableSegmentOfFinePartitions(int readBatchSize) {
        setup(readBatchSize);
        long sweepTs = TS_FINE_GRANULARITY + 1337;
        runWithConservativeSweepTimestamp(() -> writeValuesAroundSweepTimestampAndSweepAndCheck(sweepTs, 1), sweepTs);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepsOnlySweepableSegmentAcrossFinePartitionBoundary(int readBatchSize) {
        setup(readBatchSize);
        long sweepTs = TS_FINE_GRANULARITY + 7;

        // Need 2 because need to cross a partition boundary
        runWithConservativeSweepTimestamp(() -> writeValuesAroundSweepTimestampAndSweepAndCheck(sweepTs, 2), sweepTs);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepsOnlySweepableSegmentAcrossCoarsePartitionBoundary(int readBatchSize) {
        setup(readBatchSize);
        long sweepTs = TS_COARSE_GRANULARITY + 7;

        // Need 2 because need to cross a partition boundary
        runWithConservativeSweepTimestamp(() -> writeValuesAroundSweepTimestampAndSweepAndCheck(sweepTs, 2), sweepTs);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void remembersProgressWhenSweepTimestampAdvances(int readBatchSize) {
        setup(readBatchSize);
        long baseSweepTs = getSweepTsCons();
        long oldPartitionTs = baseSweepTs - 5;
        long newPartitionFirstTs = baseSweepTs + 5;
        long newPartitionSecondTs = baseSweepTs + 10;

        enqueueWriteCommitted(TABLE_CONS, oldPartitionTs);
        enqueueWriteCommitted(TABLE_CONS, newPartitionFirstTs);
        enqueueWriteCommitted(TABLE_CONS, newPartitionSecondTs);

        assertReadAtTimestampReturnsNothing(TABLE_CONS, oldPartitionTs);

        runConservativeSweepAtTimestamp(baseSweepTs + 7);
        runConservativeSweepAtTimestamp(baseSweepTs + 7);
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, oldPartitionTs + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, newPartitionFirstTs);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, newPartitionSecondTs);

        runConservativeSweepAtTimestamp(newPartitionSecondTs + 1);
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, newPartitionSecondTs);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, newPartitionSecondTs);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void doesNotGoBackwardsEvenIfSweepTimestampRegressesWithinBucket(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS2);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS3);

        runConservativeSweepAtTimestamp(LOW_TS2 + 5);
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS2);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS2);
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());
        assertProgressUpdatedToTimestamp(LOW_TS2 + 5 - 1);

        runConservativeSweepAtTimestamp(LOW_TS2 - 5);
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());
        assertProgressUpdatedToTimestamp(LOW_TS2 + 5 - 1);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void canSweepAtMinimumTimeWithNoWrites(int readBatchSize) {
        setup(readBatchSize);
        runConservativeSweepAtTimestamp(Long.MIN_VALUE);
        assertProgressUpdatedToTimestamp(SweepQueueUtils.INITIAL_TIMESTAMP);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void canSweepAtMinimumTime(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS2);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS3);

        runConservativeSweepAtTimestamp(Long.MIN_VALUE);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS);
        assertProgressUpdatedToTimestamp(SweepQueueUtils.INITIAL_TIMESTAMP);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void doesNotGoBackwardsEvenIfSweepTimestampRegressesAcrossBoundary(int readBatchSize) {
        setup(readBatchSize);
        long coarseBoundary = TS_COARSE_GRANULARITY;
        enqueueWriteCommitted(TABLE_CONS, coarseBoundary - 5);
        enqueueWriteCommitted(TABLE_CONS, coarseBoundary + 5);
        enqueueWriteCommitted(TABLE_CONS, coarseBoundary + 15);

        // Need 2 sweeps to get through the first coarse bucket
        runConservativeSweepAtTimestamp(coarseBoundary + 8);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(coarseBoundary + 8);
        runConservativeSweepAtTimestamp(coarseBoundary + 8);
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, coarseBoundary - 5 + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, coarseBoundary + 5);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, coarseBoundary + 15);

        // Now regresses (e.g. clock drift on unreadable)
        runConservativeSweepAtTimestamp(coarseBoundary - 3);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(coarseBoundary - 3);

        // And advances again
        runConservativeSweepAtTimestamp(coarseBoundary + 18);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(coarseBoundary + 18);
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, coarseBoundary - 5 + 1);
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, coarseBoundary + 5 + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, coarseBoundary + 15);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void testSweepTimestampMetric(int readBatchSize) {
        setup(readBatchSize);
        unreadableTs = 17;
        immutableTs = 40;
        sweepNextBatch(ShardAndStrategy.conservative(0));
        sweepNextBatch(ShardAndStrategy.thorough(0));

        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(17L);
        assertThat(metricsManager).hasSweepTimestampThoroughEqualTo(40);

        immutableTs = 5;
        sweepNextBatch(ShardAndStrategy.conservative(0));
        sweepNextBatch(ShardAndStrategy.thorough(0));

        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(5L);
        assertThat(metricsManager).hasSweepTimestampThoroughEqualTo(5);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void doNotSweepAnythingAfterEntryWithCommitTsAfterSweepTs(int readBatchSize) {
        setup(readBatchSize);
        immutableTs = 1000L;
        // put 4 writes committed at the same timestamp as start timestamp, and put one committed at sweep timestamp
        enqueueWriteCommitted(TABLE_CONS, 900);
        enqueueWriteCommitted(TABLE_CONS, 910);
        enqueueWriteCommitted(TABLE_CONS, 920);
        enqueueWriteCommitedAt(TABLE_CONS, 950, immutableTs);
        enqueueWriteCommitted(TABLE_CONS, 970);

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertThat(progress.getLastSweptTimestamp(ShardAndStrategy.conservative(CONS_SHARD)))
                .isEqualTo(950 - 1);
        ArgumentCaptor<Map<Cell, TimestampRangeDelete>> argument = ArgumentCaptor.forClass(Map.class);
        verify(spiedKvs, times(1)).deleteAllTimestamps(eq(TABLE_CONS), argument.capture());
        assertThat(argument.getValue())
                .containsValue(new TimestampRangeDelete.Builder()
                        .timestamp(920L)
                        .endInclusive(false)
                        .deleteSentinels(false)
                        .build());

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertThat(progress.getLastSweptTimestamp(ShardAndStrategy.conservative(CONS_SHARD)))
                .isEqualTo(950 - 1);
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());

        immutableTs = 1001L;
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertThat(progress.getLastSweptTimestamp(ShardAndStrategy.conservative(CONS_SHARD)))
                .isEqualTo(1001L - 1L);
        // we have now had a total of 2 calls to deleteAllTimestamps, 1 from before and one new
        verify(spiedKvs, times(2)).deleteAllTimestamps(eq(TABLE_CONS), argument.capture());
        assertThat(argument.getValue())
                .containsValue(new TimestampRangeDelete.Builder()
                        .timestamp(970L)
                        .endInclusive(false)
                        .deleteSentinels(false)
                        .build());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void doNotDeleteAnythingAfterEntryWithCommitTsAfterSweepTs(int readBatchSize) {
        setup(readBatchSize);
        immutableTs = 1000L;
        enqueueWriteUncommitted(TABLE_CONS, 900);
        enqueueWriteUncommitted(TABLE_CONS, 920);
        enqueueWriteCommitedAt(TABLE_CONS, 950, 2000);
        enqueueWriteUncommitted(TABLE_CONS, 970);
        enqueueWriteUncommitted(TABLE_CONS, 1110);

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertThat(progress.getLastSweptTimestamp(ShardAndStrategy.conservative(CONS_SHARD)))
                .isEqualTo(950 - 1);
        verify(spiedKvs, never()).deleteAllTimestamps(any(TableReference.class), anyMap());

        ArgumentCaptor<Multimap<Cell, Long>> multimap = ArgumentCaptor.forClass(Multimap.class);
        verify(spiedKvs, times(1)).delete(eq(TABLE_CONS), multimap.capture());
        assertThat(multimap.getValue().keySet()).containsExactly(DEFAULT_CELL);
        assertThat(multimap.getValue().values()).containsExactly(900L, 920L);

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertThat(progress.getLastSweptTimestamp(ShardAndStrategy.conservative(CONS_SHARD)))
                .isEqualTo(950 - 1);
        verify(spiedKvs, never()).deleteAllTimestamps(any(TableReference.class), anyMap());
        verify(spiedKvs, times(1)).delete(any(TableReference.class), any(Multimap.class));
        assertReadAtTimestampReturnsValue(TABLE_CONS, 1500L, 1110L);

        immutableTs = 2009L;
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertThat(progress.getLastSweptTimestamp(ShardAndStrategy.conservative(CONS_SHARD)))
                .isEqualTo(2009L - 1L);
        ArgumentCaptor<Map<Cell, TimestampRangeDelete>> map = ArgumentCaptor.forClass(Map.class);
        verify(spiedKvs, times(1)).deleteAllTimestamps(eq(TABLE_CONS), map.capture());
        assertThat(map.getValue())
                .containsValue(new TimestampRangeDelete.Builder()
                        .timestamp(950L)
                        .endInclusive(false)
                        .deleteSentinels(false)
                        .build());

        assertReadAtTimestampReturnsValue(TABLE_CONS, 1500L, 950L);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void stopReadingEarlyWhenEncounteringEntryKnownToBeCommittedAfterSweepTs(int readBatchSize) {
        setup(readBatchSize);
        immutableTs = 100L;

        enqueueWriteCommitted(TABLE_CONS, 10);
        enqueueWriteCommitedAt(TABLE_CONS, 30, 150);

        putTimestampIntoTransactionTable(50, 200);
        Map<Integer, Integer> largeWriteDistribution = enqueueAtLeastThresholdWritesInDefaultShardWithStartTs(100, 50);
        int writesInDedicated = largeWriteDistribution.get(CONS_SHARD);

        enqueueWriteUncommitted(TABLE_CONS, 70);
        enqueueWriteCommitted(TABLE_CONS, 90);

        // first iteration reads all before giving up
        assertThat(sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD))).isEqualTo(4 + writesInDedicated);
        assertThat(metricsManager).hasEntriesReadConservativeEqualTo(4 + writesInDedicated);

        // we read one entry and give up
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertThat(metricsManager).hasEntriesReadConservativeEqualTo(4 + writesInDedicated + 1);

        immutableTs = 170;

        // we read one good entry and then a reference to bad entries and give up
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertThat(metricsManager).hasEntriesReadConservativeEqualTo(4 + writesInDedicated + 3);

        immutableTs = 250;

        // we now read all to the end
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertThat(metricsManager).hasEntriesReadConservativeEqualTo(4 + writesInDedicated + 3 + writesInDedicated + 2);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void stopReadingEarlyInOtherShardWhenEncounteringEntryKnownToBeCommittedAfterSweepTs(int readBatchSize) {
        setup(readBatchSize);
        immutableTs = 100L;

        putTimestampIntoTransactionTable(50, 200);
        Map<Integer, Integer> largeWriteDistribution = enqueueAtLeastThresholdWritesInDefaultShardWithStartTs(100, 50);
        int writesInDedicated = largeWriteDistribution.get(CONS_SHARD);
        int otherShard = IntMath.mod(CONS_SHARD + 1, DEFAULT_SHARDS);
        int writesInOther = largeWriteDistribution.get(otherShard);

        assertThat(writesInOther).isGreaterThan(0);

        // first iteration reads all before giving up
        assertThat(sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD))).isEqualTo(writesInDedicated);
        assertThat(metricsManager).hasEntriesReadConservativeEqualTo(writesInDedicated);

        // we read a reference to bad entries and give up
        assertThat(sweepNextBatch(ShardAndStrategy.conservative(otherShard))).isEqualTo(1);
        assertThat(metricsManager).hasEntriesReadConservativeEqualTo(writesInDedicated + 1);

        immutableTs = 250;

        // we now read all to the end
        assertThat(sweepNextBatch(ShardAndStrategy.conservative(otherShard))).isEqualTo(writesInOther);
        assertThat(metricsManager).hasEntriesReadConservativeEqualTo(writesInDedicated + 1 + writesInOther);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void batchIncludesAllWritesWithTheSameTimestampAndDoesNotSkipOrRepeatAnyWritesInNextIteration(
            int readBatchSize) {
        setup(readBatchSize);

        SettableFuture<MultiTableSweepQueueWriter> initialisableWriter = SettableFuture.create();
        SingleBatchSweeper sweeperConservative = getSingleShardSweeper(initialisableWriter);
        MultiTableSweepQueueWriter writer = new DelegatingMultiTableSweepQueueWriter(initialisableWriter);

        int relativePrime = MAX_CELLS_GENERIC - 1;
        // this assertion verifies that the test checks what we want. If it fails, change the value of relativePrime
        assertThat(SWEEP_BATCH_SIZE % relativePrime).isNotEqualTo(0);

        int minTsToReachBatchSize = (SWEEP_BATCH_SIZE - 1) / relativePrime + 1;

        commitTransactionsWithWritesIntoUniqueCells(minTsToReachBatchSize + 1, relativePrime, writer);

        // first iteration of sweep should include all writes corresponding to timestamp 999 + minCellsToReachBatchSize,
        // since deletes are batched, we do not specify the number of calls to delete
        sweepNextBatch(sweeperConservative, ShardAndStrategy.conservative(0));
        ArgumentCaptor<Map<Cell, TimestampRangeDelete>> map = ArgumentCaptor.forClass(Map.class);
        verify(spiedKvs, atLeast(1)).deleteAllTimestamps(eq(TABLE_CONS), map.capture());
        assertThat(map.getAllValues().stream().map(Map::size).mapToInt(x -> x).sum())
                .isEqualTo(minTsToReachBatchSize * relativePrime);
        assertThat(progress.getLastSweptTimestamp(ShardAndStrategy.conservative(0)))
                .isEqualTo(1000 + minTsToReachBatchSize - 1);

        // second iteration should only contain writes corresponding to timestamp 1000 + minCellsToReachBatchSize
        sweepNextBatch(sweeperConservative, ShardAndStrategy.conservative(0));
        verify(spiedKvs, atLeast(2)).deleteAllTimestamps(eq(TABLE_CONS), map.capture());
        assertThat(map.getValue()).hasSize(relativePrime);
        assertThat(progress.getLastSweptTimestamp(ShardAndStrategy.conservative(0)))
                .isEqualTo(maxTsForFinePartition(0));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void doNotMissSingleWriteInNextIteration(int readBatchSize) {
        setup(readBatchSize);
        SettableFuture<MultiTableSweepQueueWriter> initialisableWriter = SettableFuture.create();
        SingleBatchSweeper sweeperConservative = getSingleShardSweeper(initialisableWriter);
        MultiTableSweepQueueWriter writer = new DelegatingMultiTableSweepQueueWriter(initialisableWriter);

        int minTsToReachBatchSize = (SWEEP_BATCH_SIZE - 1) / MAX_CELLS_GENERIC + 1;

        commitTransactionsWithWritesIntoUniqueCells(minTsToReachBatchSize, MAX_CELLS_GENERIC, writer);
        // put one additional transaction with a single write after
        putTimestampIntoTransactionTable(1000 + minTsToReachBatchSize, 1000 + minTsToReachBatchSize);
        Cell cell = Cell.create(PtBytes.toBytes(1000 + minTsToReachBatchSize), PtBytes.toBytes(0));
        writer.enqueue(ImmutableList.of(WriteInfo.write(TABLE_CONS, cell, 1000 + minTsToReachBatchSize)));

        // first iteration of sweep should include all but one of the writes, since deletes are batched, we do not
        // specify the number of calls to delete
        sweepNextBatch(sweeperConservative, ShardAndStrategy.conservative(0));
        ArgumentCaptor<Map<Cell, TimestampRangeDelete>> map = ArgumentCaptor.forClass(Map.class);
        verify(spiedKvs, atLeast(1)).deleteAllTimestamps(eq(TABLE_CONS), map.capture());
        assertThat(map.getAllValues().stream().map(Map::size).mapToInt(x -> x).sum())
                .isEqualTo(SWEEP_BATCH_SIZE);
        assertThat(progress.getLastSweptTimestamp(ShardAndStrategy.conservative(0)))
                .isEqualTo(1000 + minTsToReachBatchSize - 1);

        // second iteration of sweep should contain the remaining write
        sweepNextBatch(sweeperConservative, ShardAndStrategy.conservative(0));
        verify(spiedKvs, atLeast(2)).deleteAllTimestamps(eq(TABLE_CONS), map.capture());
        assertThat(map.getValue()).hasSize(1);
        assertThat(progress.getLastSweptTimestamp(ShardAndStrategy.conservative(0)))
                .isEqualTo(maxTsForFinePartition(0));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void deletesGetBatched(int readBatchSize) {
        setup(readBatchSize);
        SettableFuture<MultiTableSweepQueueWriter> initialisableWriter = SettableFuture.create();
        SingleBatchSweeper sweeperConservative = getSingleShardSweeper(initialisableWriter);
        MultiTableSweepQueueWriter writer = new DelegatingMultiTableSweepQueueWriter(initialisableWriter);
        int numberOfTimestamps = 5 * BATCH_SIZE_KVS / MAX_CELLS_GENERIC + 1;

        commitTransactionsWithWritesIntoUniqueCells(numberOfTimestamps, MAX_CELLS_GENERIC, writer);
        sweepNextBatch(sweeperConservative, ShardAndStrategy.conservative(0));
        ArgumentCaptor<Map<Cell, TimestampRangeDelete>> map = ArgumentCaptor.forClass(Map.class);
        verify(spiedKvs, times(6)).deleteAllTimestamps(eq(TABLE_CONS), map.capture());
        assertThat(map.getAllValues().stream().map(Map::size).mapToInt(x -> x).sum())
                .isEqualTo(5 * BATCH_SIZE_KVS + MAX_CELLS_GENERIC);
        assertThat(progress.getLastSweptTimestamp(ShardAndStrategy.conservative(0)))
                .isEqualTo(maxTsForFinePartition(0));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void multipleSweepersSweepDifferentShardsAndCallUnlockAfterwards(int readBatchSize)
            throws InterruptedException {
        setup(readBatchSize);

        int shards = 128;
        int sweepers = 8;
        int threads = shards / sweepers;
        TimelockService stickyLockService = createStickyLockService();
        createAndInitializeSweepersAndWaitForOneBackgroundIteration(sweepers, shards, threads, stickyLockService);

        for (int i = 0; i < shards; i++) {
            assertProgressUpdatedToTimestamp(maxTsForFinePartition(tsPartitionFine(unreadableTs - 1)), i);
            verify(stickyLockService, times(1)).unlock(ImmutableSet.of(LockToken.of(new UUID(i, 0L))));
        }

        // minimum: all threads on one host succeed, then on another, etc:
        // threads + threads * 2 + ...  + threads * swepers
        verify(stickyLockService, atLeast(threads * sweepers * (sweepers - 1) / 2))
                .lock(any(LockRequest.class));
        // maximum: all but one succeed on each host, and only then those succeed:
        // shards + shards - 1 + ... + shards - (sweepers - 1)
        verify(stickyLockService, atMost(sweepers * shards - sweepers * (sweepers - 1) / 2))
                .lock(any(LockRequest.class));
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void extraSweepersGiveUpAfterFailingToAcquireEnoughTimes(int readBatchSize) throws InterruptedException {
        setup(readBatchSize);
        int shards = 16;
        int sweepers = 4;
        int threads = shards / (sweepers / 2);
        TimelockService stickyLockService = createStickyLockService();
        createAndInitializeSweepersAndWaitForOneBackgroundIteration(sweepers, shards, threads, stickyLockService);

        ArgumentCaptor<LockRequest> captor = ArgumentCaptor.forClass(LockRequest.class);
        // minimum: as in the example above, but we have extra threads
        // threads + ... + threads * (shards / threads) + shards * (threads * sweepers - shards)
        verify(stickyLockService, atLeast(shards * (shards / threads + 1) / 2 + shards * (threads * sweepers - shards)))
                .lock(captor.capture());
        // maximum: one would think that it is
        // shards + shards - 1 + ... + shards - (sweepers - 1) + shards * (threads * sweepers - shards)
        // but actually the logic is much more complicated since threads from the same sweeper can loop back and hit a
        // race condition with each other, so we go with the more conservative upper bound
        verify(stickyLockService, atMost(threads * sweepers * shards)).lock(any());
        Set<String> requestedLockIds = captor.getAllValues().stream()
                .map(LockRequest::getLockDescriptors)
                .map(Iterables::getOnlyElement)
                .map(LockDescriptor::getLockIdAsString)
                .collect(Collectors.toSet());

        Set<String> expectedLockIds = IntStream.range(0, shards)
                .boxed()
                .map(ShardAndStrategy::conservative)
                .map(ShardAndStrategy::toText)
                .collect(Collectors.toSet());

        assertThat(requestedLockIds).hasSameElementsAs(expectedLockIds);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void doesNotLeaveSentinelsIfTableDestroyed(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_CONS, 10);
        immutableTs = 11;
        unreadableTs = 11;
        spiedKvs.truncateTable(TABLE_CONS);
        assertThat(spiedKvs.getRange(TABLE_CONS, RangeRequest.all(), Long.MAX_VALUE))
                .isExhausted();
        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertThat(spiedKvs.getRange(TABLE_CONS, RangeRequest.all(), Long.MAX_VALUE))
                .isExhausted();
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void sweepOnlyOneFinePartitionByDefault(int readBatchSize) {
        setup(readBatchSize);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS);
        enqueueTombstone(TABLE_CONS, LOW_TS + 2);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS + 4);
        enqueueTombstone(TABLE_CONS, LOW_TS + 6);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS + 8);
        // write in the next fine partition
        enqueueWriteCommitted(TABLE_CONS, maxTsForFinePartition(0) + 1);

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));

        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, maxTsForFinePartition(0) + 1);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("readBatchSizes")
    public void enableAutoTuningSweepsMultipleFinePartitions(int readBatchSize) {
        setup(readBatchSize);
        runtimeConfig.update(ImmutableTargetedSweepRuntimeConfig.builder()
                .from(runtimeConfig.get())
                .enableAutoTuning(true)
                .build());

        enqueueWriteCommitted(TABLE_CONS, LOW_TS);
        enqueueTombstone(TABLE_CONS, LOW_TS + 2);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS + 4);
        enqueueTombstone(TABLE_CONS, LOW_TS + 6);
        enqueueWriteCommitted(TABLE_CONS, LOW_TS + 8);
        // write in the next fine partition
        enqueueWriteCommitted(TABLE_CONS, maxTsForFinePartition(0) + 1);
        enqueueTombstone(TABLE_CONS, maxTsForFinePartition(0) + 2);

        sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));

        assertReadAtTimestampReturnsSentinel(TABLE_CONS, maxTsForFinePartition(0) + 1);
    }

    public void setup(int readBatchSize) {
        super.setup();

        runtimeConfig.update(ImmutableTargetedSweepRuntimeConfig.builder()
                .from(runtimeConfig.get())
                .maximumPartitionsToBatchInSingleRead(readBatchSize)
                .build());

        SettableFuture<MultiTableSweepQueueWriter> initialisableWriter = SettableFuture.create();
        targetedSweeper = TargetedSweeper.createUninitialized(
                metricsManager,
                runtimeConfig,
                installConfig,
                ImmutableList.of(),
                _unused -> {},
                spiedKvs,
                initialisableWriter,
                puncherStore);
        sweepQueue = new DelegatingMultiTableSweepQueueWriter(initialisableWriter);

        mockFollower = mock(TargetedSweepFollower.class);
        timelockService = mock(TimelockService.class);

        targetedSweeper.initializeWithoutRunning(
                timestampsSupplier, timelockService, spiedKvs, txnService, mockFollower);
        SweepQueueComponents components = targetedSweeper.components();
        sweeper = new DefaultSingleBatchSweeper(
                targetedSweeper.metrics(),
                components.shardProgress(),
                _unused -> {},
                components.reader(),
                components.deleter(),
                components.cleaner());

        progress = new ShardProgress(spiedKvs);
        sweepableTimestamps = new SweepableTimestamps(spiedKvs, partitioner);
        sweepableCells = new SweepableCells(spiedKvs, partitioner, null, txnService);
        puncherStore = KeyValueServicePuncherStore.create(spiedKvs, false);
    }

    private void sweepNextBatchForShards(SweeperStrategy sweeperStrategy, int shards) {
        for (int shard = 0; shard < shards; shard++) {
            sweepNextBatch(ShardAndStrategy.of(shard, sweeperStrategy));
        }
    }

    private void writeValuesAroundSweepTimestampAndSweepAndCheck(long sweepTimestamp, int sweepIterations) {
        enqueueWriteCommitted(TABLE_CONS, sweepTimestamp - 10);
        enqueueWriteCommitted(TABLE_CONS, sweepTimestamp - 5);
        enqueueWriteCommitted(TABLE_CONS, sweepTimestamp + 5);

        IntStream.range(0, sweepIterations)
                .forEach(unused -> sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD)));

        assertReadAtTimestampReturnsSentinel(TABLE_CONS, sweepTimestamp - 5);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, sweepTimestamp - 5);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, sweepTimestamp + 5);
    }

    private void runConservativeSweepAtTimestamp(long desiredSweepTimestamp) {
        runWithConservativeSweepTimestamp(
                () -> sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD)), desiredSweepTimestamp);
    }

    private void enqueueWriteCommitted(TableReference tableRef, long ts) {
        putTimestampIntoTransactionTable(ts, ts);
        enqueueWriteUncommitted(tableRef, ts);
    }

    private void enqueueWriteCommitedAt(TableReference tableRef, long startTs, long commitTs) {
        putTimestampIntoTransactionTable(startTs, commitTs);
        enqueueWriteUncommitted(tableRef, startTs);
    }

    private void enqueueTombstone(TableReference tableRef, long ts) {
        putTimestampIntoTransactionTable(ts, ts);
        sweepQueue.enqueue(tombstoneToDefaultCell(tableRef, ts), ts);
    }

    private void enqueueWriteUncommitted(TableReference tableRef, long startTs) {
        sweepQueue.enqueue(writeToDefaultCell(tableRef, startTs), startTs);
    }

    private Map<TableReference, ? extends Map<Cell, byte[]>> writeToDefaultCell(TableReference tableRef, long ts) {
        return writeToCell(tableRef, ts, DEFAULT_CELL);
    }

    private Map<TableReference, ? extends Map<Cell, byte[]>> writeToCell(TableReference tableRef, long ts, Cell cell) {
        Map<Cell, byte[]> singleWrite = ImmutableMap.of(cell, PtBytes.toBytes(ts));
        spiedKvs.put(tableRef, singleWrite, ts);
        return ImmutableMap.of(tableRef, singleWrite);
    }

    private Map<TableReference, ? extends Map<Cell, byte[]>> tombstoneToDefaultCell(TableReference tableRef, long ts) {
        Map<Cell, byte[]> singleWrite = ImmutableMap.of(DEFAULT_CELL, PtBytes.EMPTY_BYTE_ARRAY);
        spiedKvs.put(tableRef, singleWrite, ts);
        return ImmutableMap.of(tableRef, singleWrite);
    }

    private void assertReadAtTimestampReturnsValue(TableReference tableRef, long readTs, long value) {
        assertThat(readValueFromDefaultCell(tableRef, readTs)).isEqualTo(value);
    }

    private void assertTestValueEnqueuedAtGivenTimestampStillPresent(TableReference tableRef, long timestamp) {
        assertReadAtTimestampReturnsValue(tableRef, timestamp + 1, timestamp);
    }

    private long readValueFromDefaultCell(TableReference tableRef, long ts) {
        byte[] cellContents = Iterables.getOnlyElement(
                        readFromDefaultCell(tableRef, ts).values())
                .getContents();
        assertThat(cellContents)
                .as("Reading value from table %s at timestamp %s", tableRef, ts)
                .isNotEmpty();
        return PtBytes.toLong(cellContents);
    }

    private void assertReadAtTimestampReturnsSentinel(TableReference tableRef, long readTs) {
        assertReadAtTimestampReturnsTombstoneAtTimestamp(tableRef, readTs, -1L);
    }

    private void assertReadAtTimestampReturnsTombstoneAtTimestamp(TableReference tableRef, long readTs, long tombTs) {
        Value readValue =
                Iterables.getOnlyElement(readFromDefaultCell(tableRef, readTs).values());
        assertThat(readValue.getTimestamp()).isEqualTo(tombTs);
        assertThat(readValue.getContents()).isEmpty();
    }

    private void assertReadAtTimestampReturnsNothing(TableReference tableRef, long readTs) {
        assertThat(readFromDefaultCell(tableRef, readTs)).isEmpty();
    }

    private void assertSweepableCellsHasNoDedicatedRowsForShard(int shard) {
        TableReference sweepableCellsTable =
                TargetedSweepTableFactory.of().getSweepableCellsTable(null).getTableRef();
        try (ClosableIterator<RowResult<Value>> iterator =
                spiedKvs.getRange(sweepableCellsTable, RangeRequest.all(), Long.MAX_VALUE)) {
            assertThat(iterator.stream()
                            .map(RowResult::getRowName)
                            .map(SweepableCellsTable.SweepableCellsRow.BYTES_HYDRATOR::hydrateFromBytes)
                            .map(SweepableCellsTable.SweepableCellsRow::getMetadata)
                            .map(TargetedSweepMetadata.BYTES_HYDRATOR::hydrateFromBytes)
                            .filter(TargetedSweepMetadata::dedicatedRow)
                            .filter(metadata -> metadata.shard() == shard)
                            .collect(Collectors.toList()))
                    .isEmpty();
        }
    }

    // this implicitly assumes the entry was not committed after the timestamp
    private void assertSweepableCellsHasEntryForTimestamp(long timestamp) {
        SweepBatch batch = sweepableCells.getBatchForPartition(
                ShardAndStrategy.conservative(CONS_SHARD), tsPartitionFine(timestamp), -1L, timestamp + 1);
        assertThat(batch.writes()).containsExactly(WriteInfo.write(TABLE_CONS, DEFAULT_CELL, timestamp));
    }

    private void assertSweepableCellsHasNoEntriesInPartitionOfTimestamp(long timestamp) {
        SweepBatch batch = sweepableCells.getBatchForPartition(
                ShardAndStrategy.conservative(CONS_SHARD), tsPartitionFine(timestamp), -1L, Long.MAX_VALUE);
        assertThat(batch.writes()).isEmpty();
    }

    private Map<Cell, Value> readFromDefaultCell(TableReference tableRef, long ts) {
        Map<Cell, Long> singleRead = ImmutableMap.of(DEFAULT_CELL, ts);
        return spiedKvs.get(tableRef, singleRead);
    }

    private void assertProgressUpdatedToTimestamp(long ts) {
        assertProgressUpdatedToTimestamp(ts, CONS_SHARD);
    }

    private void assertProgressUpdatedToTimestamp(long ts, int shard) {
        assertThat(progress.getLastSweptTimestamp(ShardAndStrategy.conservative(shard)))
                .isEqualTo(ts);
    }

    private void assertLowestFinePartitionInSweepableTimestampsEquals(long partitionFine) {
        assertThat(sweepableTimestamps.nextTimestampPartition(
                        ShardAndStrategy.conservative(CONS_SHARD), -1L, getSweepTsCons()))
                .contains(partitionFine);
    }

    private void assertNoEntriesInSweepableTimestampsBeforeSweepTimestamp() {
        assertThat(sweepableTimestamps.nextTimestampPartition(
                        ShardAndStrategy.conservative(CONS_SHARD), -1L, getSweepTsCons()))
                .isEmpty();
    }

    private void setTimelockTime(long timeMillis) {
        when(timelockService.currentTimeMillis()).thenReturn(timeMillis);
    }

    private void punchTimeAtTimestamp(long timeMillis, long timestamp) {
        puncherStore.put(timestamp, timeMillis);
    }

    private void commitTransactionsWithWritesIntoUniqueCells(
            int transactions, int writes, MultiTableSweepQueueWriter writer) {
        for (int i = 1000; i < 1000 + transactions; i++) {
            putTimestampIntoTransactionTable(i, i);
            List<WriteInfo> writeInfos = new ArrayList<>();
            for (int j = 0; j < writes; j++) {
                Cell cell = Cell.create(PtBytes.toBytes(i), PtBytes.toBytes(j));
                writeInfos.add(WriteInfo.write(TABLE_CONS, cell, i));
            }
            writer.enqueue(writeInfos);
        }
    }

    private SingleBatchSweeper getSingleShardSweeper(SettableFuture<MultiTableSweepQueueWriter> initialisableWriter) {
        TargetedSweeper sweeper =
                TargetedSweeper.createUninitializedForTest(spiedKvs, Refreshable.only(1), initialisableWriter);
        sweeper.initializeWithoutRunning(
                timestampsSupplier,
                mock(TimelockService.class),
                spiedKvs,
                txnService,
                mock(TargetedSweepFollower.class));
        SweepQueueComponents components = sweeper.components();
        return new DefaultSingleBatchSweeper(
                mock(TargetedSweepMetrics.class),
                components.shardProgress(),
                _unused -> {},
                components.reader(),
                components.deleter(),
                components.cleaner());
    }

    /**
     * Creates a mock of a LockService that only gives out a lock once per unique request and never releases it, even
     * if unlock is called. The returned tokens are monotonically increasing in the tokenId.
     */
    private TimelockService createStickyLockService() {
        AtomicLong lockToken = new AtomicLong(0);
        Set<LockDescriptor> requestedLocks = new ConcurrentHashSet<>();
        TimelockService stickyLockService = mock(TimelockService.class);
        doAnswer(invocation -> {
                    LockRequest request = invocation.getArgument(0);
                    if (requestedLocks.add(Iterables.getOnlyElement(request.getLockDescriptors()))) {
                        return (LockResponse)
                                () -> Optional.of(LockToken.of(new UUID(lockToken.getAndIncrement(), 0L)));
                    } else {
                        return (LockResponse) Optional::empty;
                    }
                })
                .when(stickyLockService)
                .lock(any());
        return stickyLockService;
    }

    private void createAndInitializeSweepersAndWaitForOneBackgroundIteration(
            int sweepers, int shards, int threads, TimelockService stickyLockService) throws InterruptedException {
        TargetedSweepRuntimeConfig runtime = ImmutableTargetedSweepRuntimeConfig.builder()
                .shards(shards)
                .pauseMillis(5000)
                .build();
        TargetedSweepInstallConfig install = ImmutableTargetedSweepInstallConfig.builder()
                .conservativeThreads(threads)
                .thoroughThreads(0)
                .build();
        for (int i = 0; i < sweepers; i++) {
            TargetedSweeper sweeperInstance = TargetedSweeper.createUninitialized(
                    metricsManager,
                    Refreshable.only(runtime),
                    install,
                    ImmutableList.of(),
                    _unused -> {},
                    spiedKvs,
                    SettableFuture.create(),
                    puncherStore);
            sweeperInstance.initializeWithoutRunning(
                    timestampsSupplier, stickyLockService, spiedKvs, txnService, mockFollower);
            sweeperInstance.runInBackground();
        }
        waitUntilSweepRunsOneIteration();
    }

    private void waitUntilSweepRunsOneIteration() throws InterruptedException {
        Thread.sleep(3000L);
    }

    private Map<Integer, Integer> enqueueAtLeastThresholdWritesInDefaultShardWithStartTs(long threshold, long startTs) {
        List<WriteInfo> writeInfos = new ArrayList<>();
        int counter = 0;
        while (writeInfos.stream()
                        .filter(write -> write.toShard(DEFAULT_SHARDS) == CONS_SHARD)
                        .count()
                < threshold) {
            writeInfos.addAll(generateHundredWrites(counter++, startTs));
        }
        sweepQueue.enqueue(writeInfos);
        return writeInfos.stream()
                .collect(Collectors.toMap(write -> write.toShard(DEFAULT_SHARDS), write -> 1, Integer::sum));
    }

    private List<WriteInfo> generateHundredWrites(int startCol, long startTs) {
        List<WriteInfo> writeInfos = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            writeInfos.add(WriteInfo.write(
                    TABLE_CONS, Cell.create(DEFAULT_CELL.getRowName(), PtBytes.toBytes(startCol * 100 + i)), startTs));
        }
        return writeInfos;
    }

    private long sweepNextBatch(ShardAndStrategy shardStrategy) {
        return sweepNextBatch(sweeper, shardStrategy);
    }

    private long sweepNextBatch(SingleBatchSweeper sweeper, ShardAndStrategy shardStrategy) {
        return sweeper.sweepNextBatch(shardStrategy, Sweeper.of(shardStrategy).getSweepTimestamp(timestampsSupplier));
    }

    private void setSweepTimestamp(long timestamp) {
        immutableTs = timestamp;
        unreadableTs = timestamp;
    }

    private void await(final ThrowingRunnable assertion) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(2L))
                .with()
                .pollInterval(Duration.ofMillis(SMALL_REFRESH_MILLIS))
                .untilAsserted(assertion);
    }
}
