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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableKvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionReadInfo;
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
        assertEquals(
                ImmutableTransactionReadInfo.builder().bytesRead(0).kvsCalls(0).build(), tracker.getReadInfo());
        assertEquals(ImmutableMap.of(), tracker.getReadInfoByTable());
    }

    @Test
    public void oneReadForTableIsTracked() {
        tracker.readForTable(TABLE_1, KVS_METHOD_NAME_1, BYTES_READ_1);

        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(BYTES_READ_1)
                .kvsCalls(1)
                .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.builder()
                        .bytesRead(BYTES_READ_1)
                        .methodName(KVS_METHOD_NAME_1)
                        .build())
                .build();

        assertEquals(readInfo, tracker.getReadInfo());
        assertEquals(ImmutableMap.of(TABLE_1, readInfo), tracker.getReadInfoByTable());
    }

    @Test
    public void multipleReadsForTableAreTracked() {
        tracker.readForTable(TABLE_1, KVS_METHOD_NAME_1, BYTES_READ_1);
        tracker.readForTable(TABLE_1, KVS_METHOD_NAME_2, BYTES_READ_2);
        tracker.readForTable(TABLE_2, KVS_METHOD_NAME_2, BYTES_READ_2);
        tracker.readForTable(TABLE_3, KVS_METHOD_NAME_3, BYTES_READ_3);
        tracker.readForTable(TABLE_1, KVS_METHOD_NAME_3, NO_BYTES_READ);
        tracker.readForTable(TABLE_2, KVS_METHOD_NAME_2, BYTES_READ_3);

        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(BYTES_READ_1 + 2 * BYTES_READ_2 + 2 * BYTES_READ_3)
                .kvsCalls(6)
                .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.builder()
                        .bytesRead(BYTES_READ_3)
                        .methodName(KVS_METHOD_NAME_2)
                        .build())
                .build();

        assertEquals(readInfo, tracker.getReadInfo());

        ImmutableMap<TableReference, TransactionReadInfo> readInfoByTable =
                ImmutableMap.<TableReference, TransactionReadInfo>builder()
                        .put(
                                TABLE_1,
                                ImmutableTransactionReadInfo.builder()
                                        .bytesRead(BYTES_READ_1 + BYTES_READ_2)
                                        .kvsCalls(3)
                                        .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.builder()
                                                .bytesRead(BYTES_READ_2)
                                                .methodName(KVS_METHOD_NAME_2)
                                                .build())
                                        .build())
                        .put(
                                TABLE_2,
                                ImmutableTransactionReadInfo.builder()
                                        .bytesRead(BYTES_READ_2 + BYTES_READ_3)
                                        .kvsCalls(2)
                                        .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.builder()
                                                .bytesRead(BYTES_READ_3)
                                                .methodName(KVS_METHOD_NAME_2)
                                                .build())
                                        .build())
                        .put(
                                TABLE_3,
                                ImmutableTransactionReadInfo.builder()
                                        .bytesRead(BYTES_READ_3)
                                        .kvsCalls(1)
                                        .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.builder()
                                                .bytesRead(BYTES_READ_3)
                                                .methodName(KVS_METHOD_NAME_3)
                                                .build())
                                        .build())
                        .buildOrThrow();

        assertEquals(readInfoByTable, tracker.getReadInfoByTable());
    }

    @Test
    public void onePartialReadForTableIsTracked() {
        tracker.callForTable(TABLE_2);
        tracker.partialReadForTable(TABLE_2, BYTES_READ_2);

        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(BYTES_READ_2)
                .kvsCalls(1)
                .build();

        assertEquals(readInfo, tracker.getReadInfo());
        assertEquals(ImmutableMap.of(TABLE_2, readInfo), tracker.getReadInfoByTable());
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

        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(BYTES_READ_1 + BYTES_READ_2 + 2 * BYTES_READ_3)
                .kvsCalls(2)
                .build();

        assertEquals(readInfo, tracker.getReadInfo());
        assertEquals(ImmutableMap.of(TABLE_1, readInfo), tracker.getReadInfoByTable());
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

        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(3 * BYTES_READ_1 + BYTES_READ_2 + 3 * BYTES_READ_3)
                .kvsCalls(6)
                .build();

        assertEquals(readInfo, tracker.getReadInfo());

        ImmutableMap<TableReference, TransactionReadInfo> readInfoByTable =
                ImmutableMap.<TableReference, TransactionReadInfo>builder()
                        .put(
                                TABLE_1,
                                ImmutableTransactionReadInfo.builder()
                                        .bytesRead(BYTES_READ_1 + BYTES_READ_2)
                                        .kvsCalls(1)
                                        .build())
                        .put(
                                TABLE_2,
                                ImmutableTransactionReadInfo.builder()
                                        .bytesRead(BYTES_READ_1 + BYTES_READ_3)
                                        .kvsCalls(2)
                                        .build())
                        .put(
                                TABLE_3,
                                ImmutableTransactionReadInfo.builder()
                                        .bytesRead(BYTES_READ_1 + 2 * BYTES_READ_3)
                                        .kvsCalls(3)
                                        .build())
                        .buildOrThrow();

        assertEquals(readInfoByTable, tracker.getReadInfoByTable());
    }

    @Test
    public void oneCallForTableIsTracked() {
        tracker.callForTable(TABLE_2);
        TransactionReadInfo readInfo =
                ImmutableTransactionReadInfo.builder().bytesRead(0).kvsCalls(1).build();
        assertEquals(readInfo, tracker.getReadInfo());
        assertEquals(ImmutableMap.of(), tracker.getReadInfoByTable());
    }

    @Test
    public void multipleCallForTableAreTracked() {
        tracker.callForTable(TABLE_2);
        tracker.callForTable(TABLE_2);
        tracker.callForTable(TABLE_2);
        TransactionReadInfo readInfo =
                ImmutableTransactionReadInfo.builder().bytesRead(0).kvsCalls(3).build();
        assertEquals(readInfo, tracker.getReadInfo());
        assertEquals(ImmutableMap.of(), tracker.getReadInfoByTable());
    }

    @Test
    public void multipleCallForTableOnMultipleTablesAreTracked() {
        tracker.callForTable(TABLE_2);
        tracker.callForTable(TABLE_1);
        tracker.callForTable(TABLE_2);
        tracker.callForTable(TABLE_3);
        tracker.callForTable(TABLE_2);
        TransactionReadInfo readInfo =
                ImmutableTransactionReadInfo.builder().bytesRead(0).kvsCalls(5).build();
        assertEquals(readInfo, tracker.getReadInfo());
        assertEquals(ImmutableMap.of(), tracker.getReadInfoByTable());
    }

    @Test
    public void oneTableAgnosticReadIsTracked() {
        tracker.tableAgnosticRead(KVS_METHOD_NAME_4, BYTES_READ_3);
        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(BYTES_READ_3)
                .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.builder()
                        .bytesRead(BYTES_READ_3)
                        .methodName(KVS_METHOD_NAME_4)
                        .build())
                .kvsCalls(1)
                .build();
        assertEquals(readInfo, tracker.getReadInfo());
        assertEquals(ImmutableMap.of(), tracker.getReadInfoByTable());
    }

    @Test
    public void multipleTableAgnosticReadAreTracked() {
        tracker.tableAgnosticRead(KVS_METHOD_NAME_4, BYTES_READ_3);
        tracker.tableAgnosticRead(KVS_METHOD_NAME_5, BYTES_READ_3);
        tracker.tableAgnosticRead(KVS_METHOD_NAME_4, BYTES_READ_1);
        tracker.tableAgnosticRead(KVS_METHOD_NAME_5, BYTES_READ_2);
        tracker.tableAgnosticRead(KVS_METHOD_NAME_5, BYTES_READ_1);
        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(2 * BYTES_READ_1 + BYTES_READ_2 + 2 * BYTES_READ_3)
                .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.builder()
                        .bytesRead(BYTES_READ_3)
                        .methodName(KVS_METHOD_NAME_4)
                        .build())
                .kvsCalls(5)
                .build();
        assertEquals(readInfo, tracker.getReadInfo());
        assertEquals(ImmutableMap.of(), tracker.getReadInfoByTable());
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

        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(5 * BYTES_READ_1 + 6 * BYTES_READ_2 + 4 * BYTES_READ_3)
                .kvsCalls(12)
                .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.builder()
                        .bytesRead(BYTES_READ_3)
                        .methodName(KVS_METHOD_NAME_2)
                        .build())
                .build();

        assertEquals(readInfo, tracker.getReadInfo());

        ImmutableMap<TableReference, TransactionReadInfo> readInfoByTable =
                ImmutableMap.<TableReference, TransactionReadInfo>builder()
                        .put(
                                TABLE_1,
                                ImmutableTransactionReadInfo.builder()
                                        .bytesRead(BYTES_READ_1 + BYTES_READ_2 + 3 * BYTES_READ_3)
                                        .kvsCalls(5)
                                        .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.builder()
                                                .bytesRead(BYTES_READ_3)
                                                .methodName(KVS_METHOD_NAME_2)
                                                .build())
                                        .build())
                        .put(
                                TABLE_2,
                                ImmutableTransactionReadInfo.builder()
                                        .bytesRead(BYTES_READ_1 + 2 * BYTES_READ_2 + BYTES_READ_3)
                                        .kvsCalls(3)
                                        .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.builder()
                                                .bytesRead(BYTES_READ_3)
                                                .methodName(KVS_METHOD_NAME_3)
                                                .build())
                                        .build())
                        .put(
                                TABLE_3,
                                ImmutableTransactionReadInfo.builder()
                                        .bytesRead(2 * BYTES_READ_1 + 2 * BYTES_READ_2)
                                        .kvsCalls(2)
                                        .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.builder()
                                                .bytesRead(BYTES_READ_2)
                                                .methodName(KVS_METHOD_NAME_1)
                                                .build())
                                        .build())
                        .buildOrThrow();

        assertEquals(readInfoByTable, tracker.getReadInfoByTable());
    }
}
