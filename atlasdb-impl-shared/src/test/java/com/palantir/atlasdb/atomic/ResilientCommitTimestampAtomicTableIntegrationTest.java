/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.atomic;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.transaction.encoding.BaseProgressEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TwoPhaseEncodingStrategy;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Test;

public class ResilientCommitTimestampAtomicTableIntegrationTest {
    private static final double WRITE_FAILURE_PROBABILITY = 0.5;
    private static final long MAXIMUM_EVALUATED_TIMESTAMP = 100;

    private final ConsensusForgettingStore forgettingStore =
            new PueCassImitatingConsensusForgettingStore(WRITE_FAILURE_PROBABILITY);
    private final AtomicTable<Long, TransactionStatus> pueTable = new ResilientCommitTimestampAtomicTable(
            forgettingStore,
            new TwoPhaseEncodingStrategy(BaseProgressEncodingStrategy.INSTANCE),
            new DefaultTaggedMetricRegistry());

    @Test
    public void repeatableReads() throws InterruptedException {
        Multimap<Long, TimestampReader> timestampReaders = createStartedTimestampReaders();

        CountDownLatch writeExecutionLatch = new CountDownLatch(1);
        ExecutorService writeExecutor = Executors.newCachedThreadPool();

        for (long startTimestamp = 1; startTimestamp <= MAXIMUM_EVALUATED_TIMESTAMP; startTimestamp++) {
            for (int concurrentWriter = 1; concurrentWriter <= 10; concurrentWriter++) {
                submitWriteTask(writeExecutionLatch, writeExecutor, startTimestamp, concurrentWriter);
            }
        }

        writeExecutionLatch.countDown();
        writeExecutor.shutdown();
        boolean terminated = writeExecutor.awaitTermination(5, TimeUnit.SECONDS);
        assertThat(terminated)
                .as("must be able to resolve write contention reasonably quickly")
                .isTrue();

        timestampReaders.forEach((_startTimestamp, reader) -> reader.close());
        timestampReaders.forEach(
                ResilientCommitTimestampAtomicTableIntegrationTest::validateIndividualReaderHadRepeatableReads);
        timestampReaders
                .asMap()
                .forEach((_startTimestamp, readers) -> validateConsistencyObservedAcrossReaders(readers));
    }

    private void submitWriteTask(
            CountDownLatch writeExecutionLatch, ExecutorService writeExecutor, long startTimestamp, int writerIndex) {
        writeExecutor.submit(() -> {
            try {
                writeExecutionLatch.await();
                Uninterruptibles.sleepUninterruptibly(
                        Duration.ofMillis(ThreadLocalRandom.current().nextInt(10)));
                pueTable.update(startTimestamp, TransactionStatus.committed(startTimestamp + writerIndex));
            } catch (RuntimeException e) {
                // Expected - some failures will happen as part of our test.
            }
            return null;
        });
    }

    private static void validateIndividualReaderHadRepeatableReads(Long startTimestamp, TimestampReader reader) {
        List<Optional<Long>> reads = reader.getTimestampReads();
        Set<Optional<Long>> readSet = new HashSet<>(reads);
        assertThat(readSet)
                .as("can only read at most 2 distinct values: empty and a single fixed value")
                .hasSizeLessThanOrEqualTo(2);
        if (readSet.size() == 2) {
            Set<Optional<Long>> valuesRead =
                    readSet.stream().filter(Optional::isPresent).collect(Collectors.toSet());
            assertThat(valuesRead).as("can only read at most 1 fixed value").hasSize(1);

            Optional<Long> concreteValue = Iterables.getOnlyElement(valuesRead);
            assertThat(reads.subList(reads.indexOf(concreteValue), reads.size()))
                    .as("must always read the concrete value once it has been read")
                    .containsOnly(concreteValue);
        }
    }

    private Multimap<Long, TimestampReader> createStartedTimestampReaders() {
        Multimap<Long, TimestampReader> timestampReaders =
                MultimapBuilder.hashKeys().arrayListValues().build();
        for (long startTimestamp = 1; startTimestamp <= MAXIMUM_EVALUATED_TIMESTAMP; startTimestamp++) {
            for (int concurrentReader = 1; concurrentReader <= 5; concurrentReader++) {
                timestampReaders.put(startTimestamp, new TimestampReader(startTimestamp, pueTable));
            }
        }
        timestampReaders.forEach((_startTimestamp, reader) -> reader.start());
        return timestampReaders;
    }

    private void validateConsistencyObservedAcrossReaders(Collection<TimestampReader> readers) {
        Set<Long> concreteValuesAgreedByReaders = readers.stream()
                .map(TimestampReader::getTimestampReads)
                .flatMap(List::stream)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
        assertThat(concreteValuesAgreedByReaders)
                .as("cannot have readers individually diverge on concretely observed values")
                .hasSizeLessThanOrEqualTo(1);
    }

    private static final class TimestampReader implements AutoCloseable {
        private final long startTimestamp;
        private final AtomicTable<Long, TransactionStatus> pueTable;
        private final List<TransactionStatus> timestampReads;
        private final ScheduledExecutorService scheduledExecutorService;

        private TimestampReader(long startTimestamp, AtomicTable<Long, TransactionStatus> pueTable) {
            this.startTimestamp = startTimestamp;
            this.pueTable = pueTable;
            this.timestampReads = new CopyOnWriteArrayList<>();
            this.scheduledExecutorService = PTExecutors.newSingleThreadScheduledExecutor();
        }

        public List<Optional<Long>> getTimestampReads() {
            return timestampReads.stream()
                    .map(TransactionStatus::getCommitTimestamp)
                    .collect(Collectors.toList());
        }

        public void start() {
            scheduledExecutorService.scheduleAtFixedRate(this::readOneIteration, 0, 20, TimeUnit.MILLISECONDS);
        }

        public void readOneIteration() {
            try {
                timestampReads.add(pueTable.get(startTimestamp).get());
            } catch (Exception e) {
                // Expected - some failures will happen as part of our test.
            }
        }

        @Override
        public void close() {
            scheduledExecutorService.shutdown();
        }
    }
}
