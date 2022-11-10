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
import java.util.function.Consumer;
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
    private static final long BYTES_READ_1 = 83L;
    private static final long BYTES_READ_2 = 103L;
    private static final long BYTES_READ_3 = 971L;
    private static final int CONCURRENCY_ROUNDS = 20_000;

    private final KeyValueServiceDataTracker tracker = new KeyValueServiceDataTracker();

    @Test
    public void noReadsTracksNothing() {
        assertThat(tracker.getReadInfo()).isEqualTo(createTransactionReadInfo(0, 0));
        assertThat(tracker.getReadInfoByTable()).isEmpty();
    }

    @Test
    public void recordReadForTableCallsAreTracked() {
        tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_1, BYTES_READ_1);
        tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_2, BYTES_READ_2);
        tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_3, NO_BYTES_READ);

        tracker.recordReadForTable(TABLE_2, KVS_METHOD_NAME_2, BYTES_READ_2);
        tracker.recordReadForTable(TABLE_2, KVS_METHOD_NAME_2, BYTES_READ_3);

        tracker.recordReadForTable(TABLE_3, KVS_METHOD_NAME_3, BYTES_READ_3);

        assertThat(tracker.getReadInfo())
                .isEqualTo(createTransactionReadInfo(
                        BYTES_READ_1 + 2 * BYTES_READ_2 + 2 * BYTES_READ_3,
                        6,
                        createMaximumBytesKvsCallInfo(BYTES_READ_3, KVS_METHOD_NAME_2)));

        assertThat(tracker.getReadInfoByTable())
                .containsExactlyEntriesOf(ImmutableMap.of(
                        TABLE_1,
                                createTransactionReadInfo(
                                        BYTES_READ_1 + BYTES_READ_2,
                                        3,
                                        createMaximumBytesKvsCallInfo(BYTES_READ_2, KVS_METHOD_NAME_2)),
                        TABLE_2,
                                createTransactionReadInfo(
                                        BYTES_READ_2 + BYTES_READ_3,
                                        2,
                                        createMaximumBytesKvsCallInfo(BYTES_READ_3, KVS_METHOD_NAME_2)),
                        TABLE_3,
                                createTransactionReadInfo(
                                        BYTES_READ_3,
                                        1,
                                        createMaximumBytesKvsCallInfo(BYTES_READ_3, KVS_METHOD_NAME_3))));
    }

    @Test
    public void recordCallForTableAndInteractionsAreTracked() {
        Consumer<Long> tableOneTracker = tracker.recordCallForTable(TABLE_1);
        tableOneTracker.accept(NO_BYTES_READ);
        tableOneTracker.accept(BYTES_READ_1);
        tableOneTracker.accept(BYTES_READ_2);

        Consumer<Long> tableTwoTracker = tracker.recordCallForTable(TABLE_2);
        tableTwoTracker.accept(BYTES_READ_1);
        tableTwoTracker.accept(BYTES_READ_3);

        Consumer<Long> tableThreeFirstTracker = tracker.recordCallForTable(TABLE_3);
        tableThreeFirstTracker.accept(NO_BYTES_READ);
        tableThreeFirstTracker.accept(BYTES_READ_1);

        Consumer<Long> tableThreeSecondTracker = tracker.recordCallForTable(TABLE_3);
        tableThreeSecondTracker.accept(BYTES_READ_3);
        tableThreeSecondTracker.accept(BYTES_READ_3);

        assertThat(tracker.getReadInfo())
                .isEqualTo(createTransactionReadInfo(3 * BYTES_READ_1 + BYTES_READ_2 + 3 * BYTES_READ_3, 4));

        assertThat(tracker.getReadInfoByTable())
                .containsExactlyEntriesOf(ImmutableMap.of(
                        TABLE_1, createTransactionReadInfo(BYTES_READ_1 + BYTES_READ_2, 1),
                        TABLE_2, createTransactionReadInfo(BYTES_READ_1 + BYTES_READ_3, 1),
                        TABLE_3, createTransactionReadInfo(BYTES_READ_1 + 2 * BYTES_READ_3, 2)));
    }

    @Test
    public void recordCallForTableCallsAreTracked() {
        tracker.recordCallForTable(TABLE_1);

        tracker.recordCallForTable(TABLE_2);
        tracker.recordCallForTable(TABLE_2);
        tracker.recordCallForTable(TABLE_2);

        tracker.recordCallForTable(TABLE_3);

        assertThat(tracker.getReadInfo()).isEqualTo(createTransactionReadInfo(0, 5));
        assertThat(tracker.getReadInfoByTable())
                .containsExactlyEntriesOf(ImmutableMap.of(
                        TABLE_1, createTransactionReadInfo(0, 1),
                        TABLE_2, createTransactionReadInfo(0, 3),
                        TABLE_3, createTransactionReadInfo(0, 1)));
    }

    @Test
    public void recordTableAgnosticReadCallsAreTracked() {
        tracker.recordTableAgnosticRead(KVS_METHOD_NAME_4, BYTES_READ_1);
        tracker.recordTableAgnosticRead(KVS_METHOD_NAME_4, BYTES_READ_3);

        tracker.recordTableAgnosticRead(KVS_METHOD_NAME_5, BYTES_READ_1);
        tracker.recordTableAgnosticRead(KVS_METHOD_NAME_5, BYTES_READ_2);
        tracker.recordTableAgnosticRead(KVS_METHOD_NAME_5, BYTES_READ_3);

        assertThat(tracker.getReadInfo())
                .isEqualTo(createTransactionReadInfo(
                        2 * BYTES_READ_1 + BYTES_READ_2 + 2 * BYTES_READ_3,
                        5,
                        createMaximumBytesKvsCallInfo(BYTES_READ_3, KVS_METHOD_NAME_4)));

        assertThat(tracker.getReadInfoByTable()).isEmpty();
    }

    @Test
    public void differentMethodCallsAreTracked() {
        tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_1, BYTES_READ_1);
        tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_2, BYTES_READ_3);
        tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_3, BYTES_READ_3);
        Consumer<Long> tableOneFirstTracker = tracker.recordCallForTable(TABLE_1);
        tableOneFirstTracker.accept(BYTES_READ_3);
        Consumer<Long> tableOneSecondTracker = tracker.recordCallForTable(TABLE_1);
        tableOneSecondTracker.accept(BYTES_READ_2);

        tracker.recordReadForTable(TABLE_2, KVS_METHOD_NAME_2, BYTES_READ_2);
        tracker.recordReadForTable(TABLE_2, KVS_METHOD_NAME_3, BYTES_READ_3);
        Consumer<Long> tableTwoTracker = tracker.recordCallForTable(TABLE_2);
        tableTwoTracker.accept(BYTES_READ_1);
        tableTwoTracker.accept(BYTES_READ_2);

        tracker.recordReadForTable(TABLE_3, KVS_METHOD_NAME_1, BYTES_READ_2);
        Consumer<Long> tableThreeTracker = tracker.recordCallForTable(TABLE_3);
        tableThreeTracker.accept(BYTES_READ_1);
        tableThreeTracker.accept(BYTES_READ_1);
        tableThreeTracker.accept(BYTES_READ_2);

        tracker.recordTableAgnosticRead(KVS_METHOD_NAME_4, BYTES_READ_1);
        tracker.recordTableAgnosticRead(KVS_METHOD_NAME_5, BYTES_READ_2);

        assertThat(tracker.getReadInfo())
                .isEqualTo(createTransactionReadInfo(
                        5 * BYTES_READ_1 + 6 * BYTES_READ_2 + 4 * BYTES_READ_3,
                        12,
                        createMaximumBytesKvsCallInfo(BYTES_READ_3, KVS_METHOD_NAME_2)));

        assertThat(tracker.getReadInfoByTable())
                .containsExactlyEntriesOf(ImmutableMap.of(
                        TABLE_1,
                                createTransactionReadInfo(
                                        BYTES_READ_1 + BYTES_READ_2 + 3 * BYTES_READ_3,
                                        5,
                                        createMaximumBytesKvsCallInfo(BYTES_READ_3, KVS_METHOD_NAME_2)),
                        TABLE_2,
                                createTransactionReadInfo(
                                        BYTES_READ_1 + 2 * BYTES_READ_2 + BYTES_READ_3,
                                        3,
                                        createMaximumBytesKvsCallInfo(BYTES_READ_3, KVS_METHOD_NAME_3)),
                        TABLE_3,
                                createTransactionReadInfo(
                                        2 * BYTES_READ_1 + 2 * BYTES_READ_2,
                                        2,
                                        createMaximumBytesKvsCallInfo(BYTES_READ_2, KVS_METHOD_NAME_1))));
    }

    /**
     * Schedules tracking calls from {@link #differentMethodCallsAreTracked} repeatedly ({@link #CONCURRENCY_ROUNDS}
     * times.
     */
    @Test
    public void concurrencyAndOrderOfExecutionTest() throws InterruptedException {
        ExecutorService executor = PTExecutors.newFixedThreadPool(100);

        for (int round = 0; round < CONCURRENCY_ROUNDS; round++) {
            executor.execute(() -> tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_1, BYTES_READ_1));
            executor.execute(() -> tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_2, BYTES_READ_3));
            executor.execute(() -> tracker.recordReadForTable(TABLE_1, KVS_METHOD_NAME_3, BYTES_READ_3));
            executor.execute(() -> {
                Consumer<Long> tableOneFirstTracker = tracker.recordCallForTable(TABLE_1);
                tableOneFirstTracker.accept(BYTES_READ_3);
            });
            executor.execute(() -> {
                Consumer<Long> tableOneSecondTracker = tracker.recordCallForTable(TABLE_1);
                tableOneSecondTracker.accept(BYTES_READ_2);
            });

            executor.execute(() -> tracker.recordReadForTable(TABLE_2, KVS_METHOD_NAME_2, BYTES_READ_2));
            executor.execute(() -> tracker.recordReadForTable(TABLE_2, KVS_METHOD_NAME_3, BYTES_READ_3));
            executor.execute(() -> {
                Consumer<Long> tableTwoTracker = tracker.recordCallForTable(TABLE_2);
                tableTwoTracker.accept(BYTES_READ_1);
                tableTwoTracker.accept(BYTES_READ_2);
            });

            executor.execute(() -> tracker.recordReadForTable(TABLE_3, KVS_METHOD_NAME_1, BYTES_READ_2));
            executor.execute(() -> {
                Consumer<Long> tableThreeTracker = tracker.recordCallForTable(TABLE_3);
                tableThreeTracker.accept(BYTES_READ_1);
                tableThreeTracker.accept(BYTES_READ_1);
                tableThreeTracker.accept(BYTES_READ_2);
            });

            executor.execute(() -> tracker.recordTableAgnosticRead(KVS_METHOD_NAME_4, BYTES_READ_1));
            executor.execute(() -> tracker.recordTableAgnosticRead(KVS_METHOD_NAME_5, BYTES_READ_2));

            executor.execute(tracker::getReadInfo);
            executor.execute(tracker::getReadInfo);
            executor.execute(tracker::getReadInfo);

            executor.execute(tracker::getReadInfoByTable);
            executor.execute(tracker::getReadInfoByTable);
            executor.execute(tracker::getReadInfoByTable);
        }

        executor.shutdown();
        assertThat(executor.awaitTermination(15, TimeUnit.SECONDS)).isTrue();

        assertThat(tracker.getReadInfo())
                .isEqualTo(createTransactionReadInfo(
                        CONCURRENCY_ROUNDS * (5 * BYTES_READ_1 + 6 * BYTES_READ_2 + 4 * BYTES_READ_3),
                        CONCURRENCY_ROUNDS * 12,
                        createMaximumBytesKvsCallInfo(BYTES_READ_3, KVS_METHOD_NAME_2)));

        assertThat(tracker.getReadInfoByTable())
                .containsExactlyEntriesOf(ImmutableMap.of(
                        TABLE_1,
                        createTransactionReadInfo(
                                CONCURRENCY_ROUNDS * (BYTES_READ_1 + BYTES_READ_2 + 3 * BYTES_READ_3),
                                CONCURRENCY_ROUNDS * 5,
                                createMaximumBytesKvsCallInfo(BYTES_READ_3, KVS_METHOD_NAME_2)),
                        TABLE_2,
                        createTransactionReadInfo(
                                CONCURRENCY_ROUNDS * (BYTES_READ_1 + 2 * BYTES_READ_2 + BYTES_READ_3),
                                CONCURRENCY_ROUNDS * 3,
                                createMaximumBytesKvsCallInfo(BYTES_READ_3, KVS_METHOD_NAME_3)),
                        TABLE_3,
                        createTransactionReadInfo(
                                CONCURRENCY_ROUNDS * (2 * BYTES_READ_1 + 2 * BYTES_READ_2),
                                CONCURRENCY_ROUNDS * 2,
                                createMaximumBytesKvsCallInfo(BYTES_READ_2, KVS_METHOD_NAME_1))));
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

    private static KvsCallReadInfo createMaximumBytesKvsCallInfo(long bytesRead, String kvsMethodName) {
        return ImmutableKvsCallReadInfo.builder()
                .bytesRead(bytesRead)
                .methodName(kvsMethodName)
                .build();
    }
}
