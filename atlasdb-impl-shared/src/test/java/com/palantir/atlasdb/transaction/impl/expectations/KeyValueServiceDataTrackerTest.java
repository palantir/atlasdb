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

    private final KeyValueServiceDataTracker tracker = new KeyValueServiceDataTracker();

    @Test
    public void noReadsTracksNothing() {
        assertThat(tracker.getReadInfo()).isEqualTo(createTransactionReadInfo(0, 0));
        assertThat(tracker.getReadInfoByTable()).isEmpty();
    }

    @Test
    public void oneReadForTableIsTracked() {
        tracker.readForTable(TABLE_1, KVS_METHOD_NAME_1, BYTES_READ_1);

        TransactionReadInfo readInfo = createTransactionReadInfo(
                BYTES_READ_1, 1, createMaximumBytesKvsCallInfo(BYTES_READ_1, KVS_METHOD_NAME_1));

        assertThat(tracker.getReadInfo()).isEqualTo(readInfo);
        assertThat(tracker.getReadInfoByTable()).containsExactlyEntriesOf(ImmutableMap.of(TABLE_1, readInfo));
    }

    @Test
    public void multipleReadsForTableAreTracked() {
        tracker.readForTable(TABLE_1, KVS_METHOD_NAME_1, BYTES_READ_1);
        tracker.readForTable(TABLE_1, KVS_METHOD_NAME_2, BYTES_READ_2);
        tracker.readForTable(TABLE_2, KVS_METHOD_NAME_2, BYTES_READ_2);
        tracker.readForTable(TABLE_3, KVS_METHOD_NAME_3, BYTES_READ_3);
        tracker.readForTable(TABLE_1, KVS_METHOD_NAME_3, NO_BYTES_READ);
        tracker.readForTable(TABLE_2, KVS_METHOD_NAME_2, BYTES_READ_3);

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
    public void onePartialReadForTableIsTracked() {
        tracker.callForTable(TABLE_2);
        tracker.partialReadForTable(TABLE_2, BYTES_READ_2);

        TransactionReadInfo readInfo = createTransactionReadInfo(BYTES_READ_2, 1);

        assertThat(tracker.getReadInfo()).isEqualTo(readInfo);
        assertThat(tracker.getReadInfoByTable()).containsExactlyEntriesOf(ImmutableMap.of(TABLE_2, readInfo));
    }

    @Test
    public void multiplePartialReadForTableAreTracked() {
        tracker.callForTable(TABLE_1);
        tracker.partialReadForTable(TABLE_1, BYTES_READ_1);
        tracker.partialReadForTable(TABLE_1, NO_BYTES_READ);
        tracker.callForTable(TABLE_1);
        tracker.partialReadForTable(TABLE_1, BYTES_READ_3);
        tracker.partialReadForTable(TABLE_1, BYTES_READ_2);
        tracker.partialReadForTable(TABLE_1, BYTES_READ_3);

        TransactionReadInfo readInfo = createTransactionReadInfo(BYTES_READ_1 + BYTES_READ_2 + 2 * BYTES_READ_3, 2);

        assertThat(tracker.getReadInfo()).isEqualTo(readInfo);
        assertThat(tracker.getReadInfoByTable()).containsExactlyEntriesOf(ImmutableMap.of(TABLE_1, readInfo));
    }

    @Test
    public void multiplePartialReadForTableOnMultipleTablesAreTracked() {
        tracker.callForTable(TABLE_1);
        tracker.callForTable(TABLE_2);
        tracker.callForTable(TABLE_2);
        tracker.partialReadForTable(TABLE_1, BYTES_READ_1);
        tracker.callForTable(TABLE_3);
        tracker.partialReadForTable(TABLE_3, NO_BYTES_READ);
        tracker.partialReadForTable(TABLE_1, NO_BYTES_READ);
        tracker.callForTable(TABLE_3);
        tracker.partialReadForTable(TABLE_3, BYTES_READ_3);
        tracker.callForTable(TABLE_3);
        tracker.partialReadForTable(TABLE_3, BYTES_READ_3);
        tracker.partialReadForTable(TABLE_2, BYTES_READ_1);
        tracker.partialReadForTable(TABLE_1, BYTES_READ_2);
        tracker.partialReadForTable(TABLE_2, BYTES_READ_3);
        tracker.partialReadForTable(TABLE_3, BYTES_READ_1);

        assertThat(tracker.getReadInfo())
                .isEqualTo(createTransactionReadInfo(3 * BYTES_READ_1 + BYTES_READ_2 + 3 * BYTES_READ_3, 6));

        assertThat(tracker.getReadInfoByTable())
                .containsExactlyEntriesOf(ImmutableMap.of(
                        TABLE_1, createTransactionReadInfo(BYTES_READ_1 + BYTES_READ_2, 1),
                        TABLE_2, createTransactionReadInfo(BYTES_READ_1 + BYTES_READ_3, 2),
                        TABLE_3, createTransactionReadInfo(BYTES_READ_1 + 2 * BYTES_READ_3, 3)));
    }

    @Test
    public void oneCallForTableIsTracked() {
        tracker.callForTable(TABLE_2);

        assertThat(tracker.getReadInfo()).isEqualTo(createTransactionReadInfo(0, 1));
        assertThat(tracker.getReadInfoByTable()).isEmpty();
    }

    @Test
    public void multipleCallForTableAreTracked() {
        tracker.callForTable(TABLE_2);
        tracker.callForTable(TABLE_2);
        tracker.callForTable(TABLE_2);

        assertThat(tracker.getReadInfo()).isEqualTo(createTransactionReadInfo(0, 3));
        assertThat(tracker.getReadInfoByTable()).isEmpty();
    }

    @Test
    public void multipleCallForTableOnMultipleTablesAreTracked() {
        tracker.callForTable(TABLE_2);
        tracker.callForTable(TABLE_1);
        tracker.callForTable(TABLE_2);
        tracker.callForTable(TABLE_3);
        tracker.callForTable(TABLE_2);

        assertThat(tracker.getReadInfo()).isEqualTo(createTransactionReadInfo(0, 5));
        assertThat(tracker.getReadInfoByTable()).isEmpty();
    }

    @Test
    public void oneTableAgnosticReadIsTracked() {
        tracker.tableAgnosticRead(KVS_METHOD_NAME_4, BYTES_READ_3);

        assertThat(tracker.getReadInfo())
                .isEqualTo(createTransactionReadInfo(
                        BYTES_READ_3, 1, createMaximumBytesKvsCallInfo(BYTES_READ_3, KVS_METHOD_NAME_4)));

        assertThat(tracker.getReadInfoByTable()).isEmpty();
    }

    @Test
    public void multipleTableAgnosticReadAreTracked() {
        tracker.tableAgnosticRead(KVS_METHOD_NAME_4, BYTES_READ_3);
        tracker.tableAgnosticRead(KVS_METHOD_NAME_5, BYTES_READ_3);
        tracker.tableAgnosticRead(KVS_METHOD_NAME_4, BYTES_READ_1);
        tracker.tableAgnosticRead(KVS_METHOD_NAME_5, BYTES_READ_2);
        tracker.tableAgnosticRead(KVS_METHOD_NAME_5, BYTES_READ_1);

        assertThat(tracker.getReadInfo())
                .isEqualTo(createTransactionReadInfo(
                        2 * BYTES_READ_1 + BYTES_READ_2 + 2 * BYTES_READ_3,
                        5,
                        createMaximumBytesKvsCallInfo(BYTES_READ_3, KVS_METHOD_NAME_4)));

        assertThat(tracker.getReadInfoByTable()).isEmpty();
    }

    @Test
    public void multipleTrackingCallsMultipleTablesTest() {
        tracker.readForTable(TABLE_1, KVS_METHOD_NAME_1, BYTES_READ_1);
        tracker.tableAgnosticRead(KVS_METHOD_NAME_4, BYTES_READ_1);
        tracker.readForTable(TABLE_1, KVS_METHOD_NAME_3, BYTES_READ_3);
        tracker.callForTable(TABLE_1);
        tracker.readForTable(TABLE_1, KVS_METHOD_NAME_2, BYTES_READ_3);
        tracker.callForTable(TABLE_2);
        tracker.readForTable(TABLE_2, KVS_METHOD_NAME_2, BYTES_READ_2);
        tracker.partialReadForTable(TABLE_1, BYTES_READ_3);
        tracker.tableAgnosticRead(KVS_METHOD_NAME_5, BYTES_READ_2);
        tracker.callForTable(TABLE_1);
        tracker.partialReadForTable(TABLE_2, BYTES_READ_1);
        tracker.callForTable(TABLE_3);
        tracker.partialReadForTable(TABLE_2, BYTES_READ_2);
        tracker.partialReadForTable(TABLE_3, BYTES_READ_2);
        tracker.partialReadForTable(TABLE_1, BYTES_READ_2);
        tracker.readForTable(TABLE_2, KVS_METHOD_NAME_3, BYTES_READ_3);
        tracker.readForTable(TABLE_3, KVS_METHOD_NAME_1, BYTES_READ_2);
        tracker.partialReadForTable(TABLE_3, BYTES_READ_1);
        tracker.partialReadForTable(TABLE_3, BYTES_READ_1);

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

    private static TransactionReadInfo createTransactionReadInfo(long bytesRead, int kvsCalls) {
        return ImmutableTransactionReadInfo.builder()
                .bytesRead(bytesRead)
                .kvsCalls(kvsCalls)
                .build();
    }
}
