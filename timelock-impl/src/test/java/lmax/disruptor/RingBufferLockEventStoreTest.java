/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package lmax.disruptor;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import com.lmax.disruptor.RingBufferLockEventStore;
import com.palantir.atlasdb.timelock.lockwatches.BufferMetrics;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public final class RingBufferLockEventStoreTest {

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final RingBufferLockEventStore ringBufferLockEventStore = new RingBufferLockEventStore(
            RingBufferLockEventStore.BUFFER_SIZE, BufferMetrics.of(new DefaultTaggedMetricRegistry()));

    @AfterEach
    public void afterEach() {
        MoreExecutors.shutdownAndAwaitTermination(executorService, 1, java.util.concurrent.TimeUnit.SECONDS);
    }

    @Test
    @Disabled
    public void testGetBufferSnapshot() {
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch stopped = new CountDownLatch(1);
        Future<?> producer = executorService.submit(() -> {
            while (!stop.get()) {
                ringBufferLockEventStore.add(LockWatchCreatedEvent.builder(Set.of(), Set.of()));
            }
            stopped.countDown();
        });

        try {
            Awaitility.await("buffer to fill")
                    .until(() -> ringBufferLockEventStore.lastVersion() > RingBufferLockEventStore.BUFFER_SIZE);

            for (int i = 0; i < 5; i++) {
                long[] sequenceNumbers = Arrays.stream(ringBufferLockEventStore.getBufferSnapshot())
                        .mapToLong(LockWatchEvent::sequence)
                        .toArray();
                assertThatSequenceNumbersContiguous(sequenceNumbers);
            }
            assertThat(producer).isNotDone();

            stop.set(true);
            Uninterruptibles.awaitUninterruptibly(stopped);

            long[] sequenceNumbers = Arrays.stream(ringBufferLockEventStore.getBufferSnapshot())
                    .mapToLong(LockWatchEvent::sequence)
                    .toArray();
            assertThat(sequenceNumbers).hasSize(RingBufferLockEventStore.BUFFER_SIZE);
            assertThatSequenceNumbersContiguous(sequenceNumbers);
        } finally {
            stop.set(true);
        }
    }

    private static void assertThatSequenceNumbersContiguous(long[] sequenceNumbers) {
        long lastSequenceNumber = sequenceNumbers[0];
        for (int index = 1; index < sequenceNumbers.length; index++) {
            assertThat(sequenceNumbers[index]).isEqualTo(lastSequenceNumber + 1);
            lastSequenceNumber = sequenceNumbers[index];
        }
    }
}
