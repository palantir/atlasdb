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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.transaction.encoding.TwoPhaseEncodingStrategy;
import com.palantir.common.concurrent.PTExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.immutables.value.Value;
import org.junit.Test;

public class ResilientCommitTimestampPutUnlessExistsTableIntegrationTest {
    private static final double WRITE_FAILURE_PROBABILITY = 0.1;

    private final ConsensusForgettingStore forgettingStore =
            new CassandraImitatingConsensusForgettingStore(WRITE_FAILURE_PROBABILITY);
    private final PutUnlessExistsTable<Long, Long> pueTable =
            new ResilientCommitTimestampPutUnlessExistsTable(forgettingStore, TwoPhaseEncodingStrategy.INSTANCE);

    @Test
    public void repeatableReads() throws InterruptedException {
        ExecutorService executorService = Executors.newCachedThreadPool();
        List<Future<Object>> writerFutures = new ArrayList<>();
        List<Future<TimestampPair>> readFutures = new ArrayList<>();
        CountDownLatch writeExecutionLatch = new CountDownLatch(1);

        for (long startTimestamp = 1; startTimestamp <= 50; startTimestamp++) {
            for (int concurrentWriter = 1; concurrentWriter <= 10; concurrentWriter++) {
                int finalConcurrentWriter = concurrentWriter;
                long finalStartTimestamp = startTimestamp;

                Future<Object> writerFuture = executorService.submit(() -> {
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
                writerFutures.add(writerFuture);
            }
        }

        Multimap<Long, TimestampReader> timestampReaders = MultimapBuilder.hashKeys().arrayListValues().build();
        for (long startTimestamp = 1; startTimestamp <= 50; startTimestamp++) {
            for (int concurrentReader = 1; concurrentReader <= 10; concurrentReader++) {
                timestampReaders.put(startTimestamp, new TimestampReader(startTimestamp, pueTable));
            }
        }
        timestampReaders.forEach((_startTimestamp, reader) -> reader.start());

        writeExecutionLatch.countDown();
        executorService.shutdown();
        executorService.awaitTermination(3, TimeUnit.SECONDS);

        writerFutures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Expected - some failures will happen as part of our test.
            } catch (ExecutionException e) {
                // Expected - some failures will happen as part of our test.
            }
        });
        timestampReaders.forEach((_startTimestamp, reader) -> reader.close());
        timestampReaders.forEach((startTimestamp, reader) -> System.out.println(reader.getTimestampReads()));
    }

    static <T> Optional<T> exceptionSwallowingGet(Future<T> future) {
        try {
            T element = future.get();
            if (element != null) {
                return Optional.of(element);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Expected - some failures will happen as part of our test.
        } catch (ExecutionException e) {
            // Expected - some failures will happen as part of our test.
        }
        return Optional.empty();
    }

    private static class TimestampReader implements AutoCloseable {
        private final long startTimestamp;
        private final PutUnlessExistsTable<Long, Long> pueTable;
        private final List<OptionalLong> timestampReads;
        private final ScheduledExecutorService scheduledExecutorService;

        private TimestampReader(
                long startTimestamp,
                PutUnlessExistsTable<Long, Long> pueTable) {
            this.startTimestamp = startTimestamp;
            this.pueTable = pueTable;
            this.timestampReads = new ArrayList<>();
            this.scheduledExecutorService = PTExecutors.newSingleThreadScheduledExecutor();
        }

        public List<OptionalLong> getTimestampReads() {
            return ImmutableList.copyOf(timestampReads);
        }

        public void start() {
            scheduledExecutorService.scheduleAtFixedRate(this::readOneIteration, 0, 10, TimeUnit.MILLISECONDS);
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

    @Value.Immutable
    interface TimestampPair {
        long startTimestamp();

        OptionalLong commitTimestamp();

        static ImmutableTimestampPair.Builder builder() {
            return ImmutableTimestampPair.builder();
        }
    }
}
