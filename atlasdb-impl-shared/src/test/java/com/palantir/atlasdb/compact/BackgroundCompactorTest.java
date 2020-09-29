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
package com.palantir.atlasdb.compact;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.LockService;
import com.palantir.lock.SingleLockService;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;

public class BackgroundCompactorTest {
    private static final String TABLE_STRING = "ns.table";
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName(TABLE_STRING);

    private static final long COMPACT_PAUSE_MILLIS = 123;
    private static final long COMPACT_PAUSE_ON_FAILURE_MILLIS = 456;
    private static final long COMPACT_MINIMUM_PAUSE_ON_NOTHING_TO_COMPACT_MILLIS
            = BackgroundCompactor.SLEEP_TIME_WHEN_NOTHING_TO_COMPACT_MIN_MILLIS;
    private static final CompactorConfig COMPACTOR_CONFIG = ImmutableCompactorConfig.builder()
            .compactPauseMillis(COMPACT_PAUSE_MILLIS)
            .compactPauseOnFailureMillis(COMPACT_PAUSE_ON_FAILURE_MILLIS)
            .build();

    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    private final KeyValueService kvs = mock(KeyValueService.class);
    private final TransactionManager txManager = mock(TransactionManager.class);
    private final CompactPriorityCalculator priorityCalculator = mock(CompactPriorityCalculator.class);

    private final BackgroundCompactor compactor = new BackgroundCompactor(metricsManager,
            txManager,
            kvs,
            mock(LockService.class),
            () -> ImmutableCompactorConfig.builder()
                    .enableCompaction(true)
                    .inMaintenanceMode(true)
                    .build(),
            priorityCalculator);
    private final SingleLockService lockService = mock(SingleLockService.class);

    @Before
    public void setUp() {
        when(lockService.haveLocks()).thenReturn(true);
        when(priorityCalculator.selectTableToCompact()).thenReturn(Optional.of(TABLE_STRING));
    }

    @Test
    public void returnsLockFailureWhenCannotGetLocks() throws InterruptedException {
        SingleLockService rejectingLockService = mock(SingleLockService.class);
        when(rejectingLockService.haveLocks()).thenReturn(false);

        BackgroundCompactor.CompactionOutcome outcome = compactor.grabLockAndRunOnce(rejectingLockService);
        assertThat(outcome).isEqualTo(BackgroundCompactor.CompactionOutcome.UNABLE_TO_ACQUIRE_LOCKS);
    }

    @Test
    public void canReturnNothingToCompact() throws InterruptedException {
        when(priorityCalculator.selectTableToCompact()).thenReturn(Optional.empty());

        BackgroundCompactor.CompactionOutcome outcome = compactor.grabLockAndRunOnce(lockService);
        assertThat(outcome).isEqualTo(BackgroundCompactor.CompactionOutcome.NOTHING_TO_COMPACT);
    }

    @Test
    public void canCompactSuccessfully() throws InterruptedException {
        BackgroundCompactor.CompactionOutcome outcome = compactor.grabLockAndRunOnce(lockService);
        assertThat(outcome).isEqualTo(BackgroundCompactor.CompactionOutcome.SUCCESS);
    }

    @Test
    public void canReturnCompactionFailure() throws InterruptedException {
        doThrow(new RuntimeException())
                .when(kvs).compactInternally(TABLE, true);

        BackgroundCompactor.CompactionOutcome outcome = compactor.grabLockAndRunOnce(lockService);
        assertThat(outcome).isEqualTo(BackgroundCompactor.CompactionOutcome.FAILED_TO_COMPACT);
    }

    @Test
    public void canReturnRegistrationFailure() throws InterruptedException {
        doThrow(new RuntimeException()).when(txManager).runTaskWithRetry(any());

        BackgroundCompactor.CompactionOutcome outcome = compactor.grabLockAndRunOnce(lockService);
        assertThat(outcome).isEqualTo(BackgroundCompactor.CompactionOutcome.COMPACTED_BUT_NOT_REGISTERED);
    }

    @Test
    public void passesMaintenanceHoursCorrectly() throws InterruptedException {
        BackgroundCompactor backgroundCompactor = new BackgroundCompactor(metricsManager,
                txManager,
                kvs,
                mock(LockService.class),
                createAlternatingInMaintenanceHoursSupplier(),
                priorityCalculator);

        BackgroundCompactor.CompactionOutcome firstOutcome = backgroundCompactor.grabLockAndRunOnce(lockService);
        verify(kvs).compactInternally(TABLE, true);

        BackgroundCompactor.CompactionOutcome secondOutcome = backgroundCompactor.grabLockAndRunOnce(lockService);
        verify(kvs).compactInternally(TABLE, false);

        assertThat(firstOutcome).isEqualTo(BackgroundCompactor.CompactionOutcome.SUCCESS);
        assertThat(secondOutcome).isEqualTo(BackgroundCompactor.CompactionOutcome.SUCCESS);
        verifyNoMoreInteractions(kvs);
    }

    @Test
    public void sanityTestMetrics() {
        CompactionOutcomeMetrics metrics = new CompactionOutcomeMetrics(metricsManager);

        metrics.registerOccurrenceOf(BackgroundCompactor.CompactionOutcome.FAILED_TO_COMPACT);
        metrics.registerOccurrenceOf(BackgroundCompactor.CompactionOutcome.SUCCESS);
        metrics.registerOccurrenceOf(BackgroundCompactor.CompactionOutcome.SUCCESS);

        assertThat(metrics.getOutcomeCount(BackgroundCompactor.CompactionOutcome.SUCCESS)).isEqualTo(2L);
        assertThat(metrics.getOutcomeCount(BackgroundCompactor.CompactionOutcome.NOTHING_TO_COMPACT)).isEqualTo(0L);
        assertThat(metrics.getOutcomeCount(BackgroundCompactor.CompactionOutcome.FAILED_TO_COMPACT)).isEqualTo(1L);
    }

    @Test
    public void doesNotRunIfDisabled() throws InterruptedException {
        BackgroundCompactor backgroundCompactor = new BackgroundCompactor(metricsManager,
                txManager,
                kvs,
                mock(LockService.class),
                () -> ImmutableCompactorConfig.builder().enableCompaction(false).build(),
                priorityCalculator);

        BackgroundCompactor.CompactionOutcome outcome = backgroundCompactor.grabLockAndRunOnce(lockService);
        assertThat(outcome).isEqualTo(BackgroundCompactor.CompactionOutcome.DISABLED);
        verifyNoMoreInteractions(kvs);
    }

    @Test
    public void sleepsForShortDurationIfCompactSucceeds() {
        assertThat(BackgroundCompactor.getSleepTime(
                () -> COMPACTOR_CONFIG,
                BackgroundCompactor.CompactionOutcome.SUCCESS))
                .isEqualTo(COMPACT_PAUSE_MILLIS);
        assertThat(BackgroundCompactor.getSleepTime(
                () -> COMPACTOR_CONFIG,
                BackgroundCompactor.CompactionOutcome.COMPACTED_BUT_NOT_REGISTERED))
                .isEqualTo(COMPACT_PAUSE_MILLIS);
    }

    @Test
    public void sleepsForCompactDurationIfGreaterThanMinimumAndNothingToCompact() {
        assertThat(BackgroundCompactor.getSleepTime(
                () -> ImmutableCompactorConfig.builder()
                        .from(COMPACTOR_CONFIG)
                        .compactPauseMillis(COMPACT_MINIMUM_PAUSE_ON_NOTHING_TO_COMPACT_MILLIS + 1)
                        .build(),
                BackgroundCompactor.CompactionOutcome.NOTHING_TO_COMPACT))
                .isEqualTo(COMPACT_MINIMUM_PAUSE_ON_NOTHING_TO_COMPACT_MILLIS + 1);
    }

    @Test
    public void sleepsForAtLeastMinimumDurationIfNothingToCompact() {
        assertThat(BackgroundCompactor.getSleepTime(
                () -> ImmutableCompactorConfig.builder()
                        .from(COMPACTOR_CONFIG)
                        .compactPauseMillis(COMPACT_MINIMUM_PAUSE_ON_NOTHING_TO_COMPACT_MILLIS - 1)
                        .build(),
                BackgroundCompactor.CompactionOutcome.NOTHING_TO_COMPACT))
                .isEqualTo(COMPACT_MINIMUM_PAUSE_ON_NOTHING_TO_COMPACT_MILLIS);
    }

    @Test
    public void sleepsForLongerDurationIfCompactFails() {
        assertThat(BackgroundCompactor.getSleepTime(
                () -> COMPACTOR_CONFIG,
                BackgroundCompactor.CompactionOutcome.FAILED_TO_COMPACT))
                .isEqualTo(COMPACT_PAUSE_ON_FAILURE_MILLIS);
        assertThat(BackgroundCompactor.getSleepTime(
                () -> COMPACTOR_CONFIG,
                BackgroundCompactor.CompactionOutcome.UNABLE_TO_ACQUIRE_LOCKS))
                .isEqualTo(COMPACT_PAUSE_ON_FAILURE_MILLIS);
    }

    private Supplier<CompactorConfig> createAlternatingInMaintenanceHoursSupplier() {
        return Stream.iterate(ImmutableCompactorConfig.builder()
                        .enableCompaction(true)
                        .inMaintenanceMode(true)
                        .build(),
                oldConfig -> ImmutableCompactorConfig.builder()
                        .from(oldConfig)
                        .inMaintenanceMode(!oldConfig.inMaintenanceMode())
                        .build()).iterator()::next;
    }
}
