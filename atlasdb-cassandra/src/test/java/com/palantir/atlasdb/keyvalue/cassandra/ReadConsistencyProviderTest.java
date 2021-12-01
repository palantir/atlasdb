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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.concurrent.PTExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.Test;

public class ReadConsistencyProviderTest {
    private static final TableReference TABLE_1 = TableReference.createFromFullyQualifiedName("bobby.tables");
    private static final TableReference TABLE_2 = TableReference.createFromFullyQualifiedName("robert.tische");

    private final ReadConsistencyProvider provider = new ReadConsistencyProvider();

    @Test
    public void defaultReadConsistencyIsLocalQuorum() {
        assertThat(provider.getConsistency(TABLE_1)).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
        assertThat(provider.getConsistency(TABLE_2)).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
    }

    @Test
    public void consistencyForAtomicSerialTablesIsLocalSerial() {
        AtlasDbConstants.SERIAL_CONSISTENCY_ATOMIC_TABLES.forEach(tableReference ->
                assertThat(provider.getConsistency(tableReference)).isEqualTo(ConsistencyLevel.LOCAL_SERIAL));
    }

    @Test
    public void consistencyForAtomicSerialTablesRemainsLocalSerialEvenAfterBroadConsistencyDowngrade() {
        provider.lowerConsistencyLevelToOne();
        AtlasDbConstants.SERIAL_CONSISTENCY_ATOMIC_TABLES.forEach(tableReference ->
                assertThat(provider.getConsistency(tableReference)).isEqualTo(ConsistencyLevel.LOCAL_SERIAL));
    }

    @Test
    public void consistencyForNonSerialAtomicTablesIsLocalQuorum() {
        AtlasDbConstants.NON_SERIAL_CONSISTENCY_ATOMIC_TABLES.forEach(tableReference ->
                assertThat(provider.getConsistency(tableReference)).isEqualTo(ConsistencyLevel.LOCAL_QUORUM));
    }

    @Test
    public void canLowerConsistencyToOne() {
        assertThat(provider.getConsistency(TABLE_1)).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
        provider.lowerConsistencyLevelToOne();
        assertThat(provider.getConsistency(TABLE_1)).isEqualTo(ConsistencyLevel.ONE);
    }

    @Test
    public void loweringConsistencyIsIdempotent() {
        provider.lowerConsistencyLevelToOne();
        assertThatCode(provider::lowerConsistencyLevelToOne).doesNotThrowAnyException();
        assertThat(provider.getConsistency(TABLE_1)).isEqualTo(ConsistencyLevel.ONE);
    }

    @Test
    public void separateProvidersHaveSeparateLifecycles() {
        ReadConsistencyProvider anotherProvider = new ReadConsistencyProvider();
        anotherProvider.lowerConsistencyLevelToOne();
        assertThat(anotherProvider.getConsistency(TABLE_1)).isEqualTo(ConsistencyLevel.ONE);
        assertThat(provider.getConsistency(TABLE_1)).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
    }

    @Test
    public void concurrentUpdatesAndReadsPermitted() throws InterruptedException {
        ExecutorService executorService = PTExecutors.newCachedThreadPool();
        List<Future<ConsistencyLevel>> readFutures = new ArrayList<>();
        List<Future<?>> consistencyLevelLoweringFutures = new ArrayList<>();
        CountDownLatch latchBlockedOnThirtyCompletedReads = new CountDownLatch(30);
        CountDownLatch latchBlockedOnOneCompletedLowering = new CountDownLatch(1);
        for (int index = 0; index < 50; index++) {
            readFutures.add(executorService.submit(() -> {
                ConsistencyLevel level = provider.getConsistency(TABLE_1);
                latchBlockedOnThirtyCompletedReads.countDown();
                return level;
            }));
        }
        for (int index = 0; index < 50; index++) {
            readFutures.add(executorService.submit(() -> {
                latchBlockedOnOneCompletedLowering.await();
                return provider.getConsistency(TABLE_1);
            }));
        }
        for (int index = 0; index < 5; index++) {
            consistencyLevelLoweringFutures.add(executorService.submit(() -> {
                try {
                    latchBlockedOnThirtyCompletedReads.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                provider.lowerConsistencyLevelToOne();
                latchBlockedOnOneCompletedLowering.countDown();
            }));
        }
        executorService.shutdown();
        boolean successfulShutdown = executorService.awaitTermination(5, TimeUnit.SECONDS);
        assertThat(successfulShutdown).isTrue();

        Map<ConsistencyLevel, Long> consistencyLevelReadCount = readFutures.stream()
                .map(Futures::getUnchecked)
                .collect(Collectors.groupingBy(x -> x, Collectors.counting()));
        assertThat(consistencyLevelReadCount.keySet())
                .containsExactlyInAnyOrder(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.ONE);
        assertThat(consistencyLevelReadCount.get(ConsistencyLevel.LOCAL_QUORUM)).isGreaterThanOrEqualTo(30);
        assertThat(consistencyLevelReadCount.get(ConsistencyLevel.ONE)).isGreaterThanOrEqualTo(50);
        assertThat(consistencyLevelReadCount.values().stream().mapToLong(x -> x).sum())
                .isEqualTo(100);

        consistencyLevelLoweringFutures.forEach(
                future -> assertThat(future.isDone()).isTrue());
    }
}
