/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.expectations;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableKvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.KvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import com.palantir.common.concurrent.PTExecutors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public final class KeyValueServiceDataTrackerTest {
    private static final TableReference TABLE_1 = TableReference.createWithEmptyNamespace("Table1");
    private static final TableReference TABLE_2 = TableReference.createWithEmptyNamespace("Table2");
    private static final TableReference TABLE_3 = TableReference.createWithEmptyNamespace("Table3");
    private static final String KVS_METHOD_NAME_1 = "getRows";
    private static final String KVS_METHOD_NAME_2 = "getAsync";
    private static final String KVS_METHOD_NAME_3 = "get";
    private static final String KVS_METHOD_NAME_4 = "getMetadataForTables";
    private static final String KVS_METHOD_NAME_5 = "getMetadataForTable";
    private static final long NO_BYTES_READ = 0L;
    private static final long SMALL_BYTES_READ = 83L;
    private static final long MEDIUM_BYTES_READ = 103L;
    private static final long HIGH_LARGE_BYTES = 971L;
    private static final int CONCURRENCY_ROUNDS = 20_000;

    private final KeyValueServiceDataTracker tracker = new KeyValueServiceDataTracker();

    @Test
    public void noReadsTracksNothing() {
        assertThat(tracker.getReadInfo()).isEqualTo(createTransactionReadInfo(0, 0));
        assertThat(tracker.getReadInfoByTable()).isEmpty();
    }

    @Test
    public void recordReadForTableCallsAreTracked() {
        int numberOfKvsCallsForTable1 = 3;
        tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_3, NO_BYTES_READ);
        tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_1, SMALL_BYTES_READ);
        tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_2, MEDIUM_BYTES_READ);

        int numberOfKvsCallsForTable2 = 2;
        tracker.recordReadForTable(TABLE_2, KVS_METHOD_NAME_2, MEDIUM_BYTES_READ);
        tracker.recordReadForTable(TABLE_2, KVS_METHOD_NAME_2, HIGH_LARGE_BYTES);

        int numberOfKvsCallsForTable3 = 1;
        tracker.recordReadForTable(TABLE_3, KVS_METHOD_NAME_3, HIGH_LARGE_BYTES);

        int totalNumberOfKvsCalls = numberOfKvsCallsForTable1 + numberOfKvsCallsForTable2 + numberOfKvsCallsForTable3;

        assertThat(tracker.getReadInfo())
                .isEqualTo(createTransactionReadInfo(
                        SMALL_BYTES_READ + 2 * MEDIUM_BYTES_READ + 2 * HIGH_LARGE_BYTES,
                        totalNumberOfKvsCalls,
                        ImmutableKvsCallReadInfo.of(KVS_METHOD_NAME_2, HIGH_LARGE_BYTES)));

        assertThat(tracker.getReadInfoByTable())
                .containsExactlyEntriesOf(ImmutableMap.of(
                        TABLE_1,
                                createTransactionReadInfo(
                                        SMALL_BYTES_READ + MEDIUM_BYTES_READ,
                                        numberOfKvsCallsForTable1,
                                        ImmutableKvsCallReadInfo.of(KVS_METHOD_NAME_2, MEDIUM_BYTES_READ)),
                        TABLE_2,
                                createTransactionReadInfo(
                                        MEDIUM_BYTES_READ + HIGH_LARGE_BYTES,
                                        numberOfKvsCallsForTable2,
                                        ImmutableKvsCallReadInfo.of(KVS_METHOD_NAME_2, HIGH_LARGE_BYTES)),
                        TABLE_3,
                                createTransactionReadInfo(
                                        HIGH_LARGE_BYTES,
                                        numberOfKvsCallsForTable3,
                                        ImmutableKvsCallReadInfo.of(KVS_METHOD_NAME_3, HIGH_LARGE_BYTES))));
    }

    @Test
    public void recordCallForTableAndInteractionsAreTracked() {
        int numberOfKvsCallsForTable1 = 1;
        BytesReadTracker tableOneTracker = tracker.recordCallForTable(TABLE_1);
        tableOneTracker.record(NO_BYTES_READ);
        tableOneTracker.record(SMALL_BYTES_READ);
        tableOneTracker.record(MEDIUM_BYTES_READ);

        int numberOfKvsCallsForTable2 = 1;
        tracker.recordCallForTable(TABLE_2);

        int numberOfKvsCallsForTable3 = 2;
        BytesReadTracker tableThreeFirstTracker = tracker.recordCallForTable(TABLE_3);
        tableThreeFirstTracker.record(NO_BYTES_READ);
        tableThreeFirstTracker.record(SMALL_BYTES_READ);
        BytesReadTracker tableThreeSecondTracker = tracker.recordCallForTable(TABLE_3);
        tableThreeSecondTracker.record(HIGH_LARGE_BYTES);
        tableThreeSecondTracker.record(HIGH_LARGE_BYTES);

        int totalNumberOfKvsCalls = numberOfKvsCallsForTable1 + numberOfKvsCallsForTable2 + numberOfKvsCallsForTable3;

        assertThat(tracker.getReadInfo())
                .isEqualTo(createTransactionReadInfo(
                        2 * SMALL_BYTES_READ + MEDIUM_BYTES_READ + 2 * HIGH_LARGE_BYTES, totalNumberOfKvsCalls));

        assertThat(tracker.getReadInfoByTable())
                .containsExactlyEntriesOf(ImmutableMap.of(
                        TABLE_1,
                                createTransactionReadInfo(
                                        SMALL_BYTES_READ + MEDIUM_BYTES_READ, numberOfKvsCallsForTable1),
                        TABLE_2, createTransactionReadInfo(0, numberOfKvsCallsForTable2),
                        TABLE_3,
                                createTransactionReadInfo(
                                        SMALL_BYTES_READ + 2 * HIGH_LARGE_BYTES, numberOfKvsCallsForTable3)));
    }

    @Test
    public void recordTableAgnosticReadCallsAreTracked() {
        tracker.recordTableAgnosticRead(KVS_METHOD_NAME_4, SMALL_BYTES_READ);
        tracker.recordTableAgnosticRead(KVS_METHOD_NAME_4, HIGH_LARGE_BYTES);

        tracker.recordTableAgnosticRead(KVS_METHOD_NAME_5, SMALL_BYTES_READ);
        tracker.recordTableAgnosticRead(KVS_METHOD_NAME_5, MEDIUM_BYTES_READ);
        tracker.recordTableAgnosticRead(KVS_METHOD_NAME_5, HIGH_LARGE_BYTES);

        assertThat(tracker.getReadInfo())
                .isEqualTo(createTransactionReadInfo(
                        2 * SMALL_BYTES_READ + MEDIUM_BYTES_READ + 2 * HIGH_LARGE_BYTES,
                        5,
                        ImmutableKvsCallReadInfo.of(KVS_METHOD_NAME_4, HIGH_LARGE_BYTES)));

        assertThat(tracker.getReadInfoByTable()).isEmpty();
    }

    @Test
    public void differentMethodCallsAreTracked() {
        tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_1, SMALL_BYTES_READ);
        BytesReadTracker bytesReadTracker = tracker.recordCallForTable(TABLE_1);
        bytesReadTracker.record(MEDIUM_BYTES_READ);

        tracker.recordTableAgnosticRead(KVS_METHOD_NAME_5, HIGH_LARGE_BYTES);

        assertThat(tracker.getReadInfo())
                .isEqualTo(createTransactionReadInfo(
                        SMALL_BYTES_READ + MEDIUM_BYTES_READ + HIGH_LARGE_BYTES,
                        3,
                        ImmutableKvsCallReadInfo.of(KVS_METHOD_NAME_5, HIGH_LARGE_BYTES)));

        assertThat(tracker.getReadInfoByTable())
                .containsExactlyEntriesOf(ImmutableMap.of(
                        TABLE_1,
                        createTransactionReadInfo(
                                SMALL_BYTES_READ + MEDIUM_BYTES_READ,
                                2,
                                ImmutableKvsCallReadInfo.of(KVS_METHOD_NAME_1, SMALL_BYTES_READ))));
    }

    /**
     * Schedules tracking calls from {@link #differentMethodCallsAreTracked} repeatedly ({@link #CONCURRENCY_ROUNDS}
     * times.
     */
    @Test
    public void interleavedMethodCallsAreTracked() throws InterruptedException {
        ExecutorService executor = PTExecutors.newFixedThreadPool(100);

        AtomicInteger exceptionsSeen = new AtomicInteger(0);

        for (int round = 0; round < CONCURRENCY_ROUNDS; round++) {
            executor.execute(wrapForExceptionTracking(
                    () -> tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_1, SMALL_BYTES_READ), exceptionsSeen));
            executor.execute(wrapForExceptionTracking(
                    () -> tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_2, HIGH_LARGE_BYTES), exceptionsSeen));
            executor.execute(wrapForExceptionTracking(
                    () -> tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_3, HIGH_LARGE_BYTES), exceptionsSeen));
            executor.execute(wrapForExceptionTracking(
                    () -> {
                        BytesReadTracker tableOneFirstTracker = tracker.recordCallForTable(TABLE_1);
                        tableOneFirstTracker.record(HIGH_LARGE_BYTES);
                    },
                    exceptionsSeen));
            executor.execute(wrapForExceptionTracking(
                    () -> {
                        BytesReadTracker tableOneSecondTracker = tracker.recordCallForTable(TABLE_1);
                        tableOneSecondTracker.record(MEDIUM_BYTES_READ);
                    },
                    exceptionsSeen));

            executor.execute(wrapForExceptionTracking(
                    () -> tracker.recordReadForTable(TABLE_2, KVS_METHOD_NAME_2, MEDIUM_BYTES_READ), exceptionsSeen));
            executor.execute(wrapForExceptionTracking(
                    () -> tracker.recordReadForTable(TABLE_2, KVS_METHOD_NAME_3, HIGH_LARGE_BYTES), exceptionsSeen));
            executor.execute(wrapForExceptionTracking(
                    () -> {
                        BytesReadTracker tableTwoTracker = tracker.recordCallForTable(TABLE_2);
                        tableTwoTracker.record(SMALL_BYTES_READ);
                        tableTwoTracker.record(MEDIUM_BYTES_READ);
                    },
                    exceptionsSeen));

            executor.execute(wrapForExceptionTracking(
                    () -> tracker.recordReadForTable(TABLE_3, KVS_METHOD_NAME_1, MEDIUM_BYTES_READ), exceptionsSeen));
            executor.execute(wrapForExceptionTracking(
                    () -> {
                        BytesReadTracker tableThreeTracker = tracker.recordCallForTable(TABLE_3);
                        tableThreeTracker.record(SMALL_BYTES_READ);
                        tableThreeTracker.record(SMALL_BYTES_READ);
                        tableThreeTracker.record(MEDIUM_BYTES_READ);
                    },
                    exceptionsSeen));

            executor.execute(wrapForExceptionTracking(
                    () -> tracker.recordTableAgnosticRead(KVS_METHOD_NAME_4, SMALL_BYTES_READ), exceptionsSeen));
            executor.execute(wrapForExceptionTracking(
                    () -> tracker.recordTableAgnosticRead(KVS_METHOD_NAME_5, MEDIUM_BYTES_READ), exceptionsSeen));

            executor.execute(wrapForExceptionTracking(tracker::getReadInfo, exceptionsSeen));
            executor.execute(wrapForExceptionTracking(tracker::getReadInfo, exceptionsSeen));
            executor.execute(wrapForExceptionTracking(tracker::getReadInfo, exceptionsSeen));

            executor.execute(wrapForExceptionTracking(tracker::getReadInfoByTable, exceptionsSeen));
            executor.execute(wrapForExceptionTracking(tracker::getReadInfoByTable, exceptionsSeen));
            executor.execute(wrapForExceptionTracking(tracker::getReadInfoByTable, exceptionsSeen));
        }

        executor.shutdown();
        assertThat(executor.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
        assertThat(exceptionsSeen).hasValue(0);

        assertThat(tracker.getReadInfo())
                .isEqualTo(createTransactionReadInfo(
                        CONCURRENCY_ROUNDS * (5 * SMALL_BYTES_READ + 6 * MEDIUM_BYTES_READ + 4 * HIGH_LARGE_BYTES),
                        CONCURRENCY_ROUNDS * 12,
                        ImmutableKvsCallReadInfo.of(KVS_METHOD_NAME_2, HIGH_LARGE_BYTES)));

        assertThat(tracker.getReadInfoByTable())
                .containsExactlyEntriesOf(ImmutableMap.of(
                        TABLE_1,
                        createTransactionReadInfo(
                                CONCURRENCY_ROUNDS * (SMALL_BYTES_READ + MEDIUM_BYTES_READ + 3 * HIGH_LARGE_BYTES),
                                CONCURRENCY_ROUNDS * 5,
                                ImmutableKvsCallReadInfo.of(KVS_METHOD_NAME_2, HIGH_LARGE_BYTES)),
                        TABLE_2,
                        createTransactionReadInfo(
                                CONCURRENCY_ROUNDS * (SMALL_BYTES_READ + 2 * MEDIUM_BYTES_READ + HIGH_LARGE_BYTES),
                                CONCURRENCY_ROUNDS * 3,
                                ImmutableKvsCallReadInfo.of(KVS_METHOD_NAME_3, HIGH_LARGE_BYTES)),
                        TABLE_3,
                        createTransactionReadInfo(
                                CONCURRENCY_ROUNDS * (2 * SMALL_BYTES_READ + 2 * MEDIUM_BYTES_READ),
                                CONCURRENCY_ROUNDS * 2,
                                ImmutableKvsCallReadInfo.of(KVS_METHOD_NAME_1, MEDIUM_BYTES_READ))));
    }

    private static Runnable wrapForExceptionTracking(Runnable task, AtomicInteger exceptionsSeen) {
        return () -> {
            try {
                task.run();
            } catch (Throwable throwable) {
                exceptionsSeen.incrementAndGet();
            }
        };
    }

    private static TransactionReadInfo createTransactionReadInfo(long bytesRead, int kvsCalls) {
        return ImmutableTransactionReadInfo.builder()
                .bytesRead(bytesRead)
                .kvsCalls(kvsCalls)
                .build();
    }

    private static TransactionReadInfo createTransactionReadInfo(
            long bytesRead, long kvsCalls, KvsCallReadInfo maximumBytesKvsCallInfo) {
        return ImmutableTransactionReadInfo.builder()
                .bytesRead(bytesRead)
                .kvsCalls(kvsCalls)
                .maximumBytesKvsCallInfo(maximumBytesKvsCallInfo)
                .build();
    }
}
