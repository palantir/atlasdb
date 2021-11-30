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

package com.palantir.atlasdb.pue;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.transaction.encoding.TwoPhaseEncodingStrategy;
import com.palantir.common.concurrent.PTExecutors;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Test;

public class ResilientCommitTimestampPutUnlessExistsTableIntegrationTest {
    private static final double WRITE_FAILURE_PROBABILITY = 0.5;
    private static final long MAXIMUM_EVALUATED_TIMESTAMP = 100;

    private final ConsensusForgettingStore forgettingStore =
            new CassandraImitatingConsensusForgettingStore(WRITE_FAILURE_PROBABILITY);
    private final PutUnlessExistsTable<Long, Long> pueTable =
            new ResilientCommitTimestampPutUnlessExistsTable(forgettingStore, TwoPhaseEncodingStrategy.INSTANCE);

    @Test
    public void repeatableReads() throws InterruptedException {
        Multimap<Long, TimestampReader> timestampReaders = createStartedTimestampReaders();

        CountDownLatch writeExecutionLatch = new CountDownLatch(1);
        ExecutorService writeExecutor = Executors.newCachedThreadPool();

        for (long startTimestamp = 1; startTimestamp <= MAXIMUM_EVALUATED_TIMESTAMP; startTimestamp++) {
            for (int concurrentWriter = 1; concurrentWriter <= 10; concurrentWriter++) {
                int finalConcurrentWriter = concurrentWriter;
                long finalStartTimestamp = startTimestamp;

                writeExecutor.submit(() -> {
                    try {
                        writeExecutionLatch.await();
                        Uninterruptibles.sleepUninterruptibly(
                                ThreadLocalRandom.current().nextInt(10), TimeUnit.MILLISECONDS);
                        pueTable.putUnlessExists(finalStartTimestamp, finalStartTimestamp + finalConcurrentWriter);
                    } catch (RuntimeException e) {
                        // Expected - some failures will happen as part of our test.
                    }
                    return null;
                });
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
                ResilientCommitTimestampPutUnlessExistsTableIntegrationTest
                        ::validateIndividualReaderHadRepeatableReads);
        timestampReaders
                .asMap()
                .forEach((_startTimestamp, readers) -> validateConsistencyObservedAcrossReaders(readers));
    }

    private static void validateIndividualReaderHadRepeatableReads(Long startTimestamp, TimestampReader reader) {
        List<OptionalLong> reads = reader.getTimestampReads();
        Set<OptionalLong> readSet = new HashSet<>(reads);
        assertThat(readSet)
                .as("can only read at most 2 distinct values: empty and a single fixed value")
                .hasSizeLessThanOrEqualTo(2);
        if (readSet.size() == 2) {
            Set<OptionalLong> valuesRead =
                    readSet.stream().filter(OptionalLong::isPresent).collect(Collectors.toSet());
            assertThat(valuesRead).as("can only read at most 1 fixed value").hasSize(1);

            OptionalLong concreteValue = Iterables.getOnlyElement(valuesRead);
            assertThat(reads.subList(0, reads.indexOf(concreteValue)))
                    .as("must only read empty before a concrete value has been read")
                    .containsOnly(OptionalLong.empty());
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
        Set<OptionalLong> concreteValuesAgreedByReaders = readers.stream()
                .map(TimestampReader::getTimestampReads)
                .filter(list -> !list.isEmpty())
                .map(reads -> reads.get(reads.size() - 1))
                .filter(OptionalLong::isPresent)
                .collect(Collectors.toSet());
        assertThat(concreteValuesAgreedByReaders)
                .as("cannot have readers individually diverge on concretely observed values")
                .hasSizeLessThanOrEqualTo(1);
    }

    private static final class TimestampReader implements AutoCloseable {
        private final long startTimestamp;
        private final PutUnlessExistsTable<Long, Long> pueTable;
        private final List<OptionalLong> timestampReads;
        private final ScheduledExecutorService scheduledExecutorService;

        private TimestampReader(long startTimestamp, PutUnlessExistsTable<Long, Long> pueTable) {
            this.startTimestamp = startTimestamp;
            this.pueTable = pueTable;
            this.timestampReads = new ArrayList<>();
            this.scheduledExecutorService = PTExecutors.newSingleThreadScheduledExecutor();
        }

        public List<OptionalLong> getTimestampReads() {
            return ImmutableList.copyOf(timestampReads);
        }

        public void start() {
            scheduledExecutorService.scheduleAtFixedRate(this::readOneIteration, 0, 20, TimeUnit.MILLISECONDS);
        }

        public void readOneIteration() {
            try {
                Long rawRead = pueTable.get(startTimestamp).get();
                if (rawRead == null) {
                    timestampReads.add(OptionalLong.empty());
                } else {
                    timestampReads.add(OptionalLong.of(rawRead));
                }
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
