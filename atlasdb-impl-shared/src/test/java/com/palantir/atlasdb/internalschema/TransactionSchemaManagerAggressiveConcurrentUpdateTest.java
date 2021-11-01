/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.internalschema;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.util.MetricsManagers;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class TransactionSchemaManagerAggressiveConcurrentUpdateTest {
    private static final int NUM_THREADS = 8;

    private final ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS);
    private final KeyValueService kvs = new InMemoryKeyValueService(true);
    private final AtomicLong localTimestamp = new AtomicLong(1L);

    @Test
    public void reasonableStateAfterAggressiveConcurrentUpdatesToOneManager() {
        scheduleTasksAndValidateSnapshots(500, 1);
    }

    @Test
    public void multipleManagersInAgreementAfterAggressiveConcurrentUpdates() {
        scheduleTasksAndValidateSnapshots(1000, 10);
    }

    private void scheduleTasksAndValidateSnapshots(int numRequests, int numManagers) {
        List<TransactionSchemaManager> managers = IntStream.range(0, numManagers)
                .mapToObj(_unused -> createTransactionSchemaManager())
                .collect(Collectors.toList());

        List<Future<?>> futures = new ArrayList<>();
        Set<ValueAndBound<TimestampPartitioningMap<Integer>>> snapshots = ConcurrentHashMap.newKeySet();

        for (int i = 0; i < numRequests; i++) {
            futures.add(service.submit(() -> writeBetweenTwoSnapshots(getRandomManager(managers), snapshots)));
        }
        futures.forEach(Futures::getUnchecked);
        validateSnapshots(snapshots);
    }

    private TransactionSchemaManager createTransactionSchemaManager() {
        return new TransactionSchemaManager(CoordinationServices.createDefault(
                kvs, localTimestamp::getAndIncrement, MetricsManagers.createForTests(), false));
    }

    private static TransactionSchemaManager getRandomManager(List<TransactionSchemaManager> managers) {
        return managers.get(ThreadLocalRandom.current().nextInt(managers.size()));
    }

    private static void writeBetweenTwoSnapshots(
            TransactionSchemaManager manager, Set<ValueAndBound<TimestampPartitioningMap<Integer>>> snapshots) {
        readCoordinationAndStoreTimestampMap(manager, snapshots);
        manager.tryInstallNewTransactionsSchemaVersion(
                ThreadLocalRandom.current().nextInt(10));
        readCoordinationAndStoreTimestampMap(manager, snapshots);
    }

    private static void readCoordinationAndStoreTimestampMap(
            TransactionSchemaManager manager, Set<ValueAndBound<TimestampPartitioningMap<Integer>>> snapshots) {
        Optional<ValueAndBound<InternalSchemaMetadata>> optionalLocalValue =
                manager.getCoordinationService().getLastKnownLocalValue();
        optionalLocalValue.ifPresent(valueAndBound -> valueAndBound
                .value()
                .ifPresent(value -> snapshots.add(
                        ValueAndBound.of(value.timestampToTransactionsTableSchemaVersion(), valueAndBound.bound()))));
    }

    /**
     * Validates that a set of {@link ValueAndBound}s of {@link TimestampPartitioningMap}s is internally consistent.
     * That is, there is a linear ordering on the values.
     *
     * @param snapshots snapshots to validate
     */
    private static void validateSnapshots(Set<ValueAndBound<TimestampPartitioningMap<Integer>>> snapshots) {
        validateUniqueness(snapshots);
        validateOrdering(snapshots);
    }

    private static void validateUniqueness(Set<ValueAndBound<TimestampPartitioningMap<Integer>>> snapshots) {
        boolean unique = snapshots.stream()
                .map(ValueAndBound::bound)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .values()
                .stream()
                .allMatch(size -> size == 1);
        assertThat(unique).isTrue();
    }

    private static void validateOrdering(Set<ValueAndBound<TimestampPartitioningMap<Integer>>> snapshots) {
        TimestampPartitioningMap<Integer> finalSnapshot = getFinalSnapshot(snapshots);

        for (ValueAndBound<TimestampPartitioningMap<Integer>> valueAndBound : snapshots) {
            long comparisonBound = valueAndBound.bound();
            valueAndBound
                    .value()
                    .ifPresent(currentSnapshot ->
                            assertEqualityUpToBound(finalSnapshot, currentSnapshot, comparisonBound));
        }
    }

    private static TimestampPartitioningMap<Integer> getFinalSnapshot(
            Set<ValueAndBound<TimestampPartitioningMap<Integer>>> snapshots) {
        ValueAndBound<TimestampPartitioningMap<Integer>> maximumBoundedValue = snapshots.stream()
                .max(Comparator.comparingLong(ValueAndBound::bound))
                .orElseThrow(() -> new IllegalStateException("No maximum value found, unexpected"));
        return maximumBoundedValue.value().orElseThrow(() -> new IllegalStateException("Last snapshot is empty!"));
    }

    private static void assertEqualityUpToBound(
            TimestampPartitioningMap<Integer> finalSnapshot,
            TimestampPartitioningMap<Integer> currentSnapshot,
            long bound) {
        RangeMap<Long, Integer> snapshotSubRange = getSubRangeMap(currentSnapshot, bound);
        RangeMap<Long, Integer> lastSnapshotSubRange = getSubRangeMap(finalSnapshot, bound);
        assertThat(snapshotSubRange).isEqualTo(lastSnapshotSubRange);
    }

    private static RangeMap<Long, Integer> getSubRangeMap(TimestampPartitioningMap<Integer> snapshot, long bound) {
        return snapshot.rangeMapView().subRangeMap(Range.atMost(bound));
    }
}
