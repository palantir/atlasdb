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

public class KeyValueServiceDataTrackerTest {

    private static final TableReference TABLE_1 = TableReference.createWithEmptyNamespace("Table1");
    private static final TableReference TABLE_2 = TableReference.createWithEmptyNamespace("Table2");
    private static final TableReference TABLE_3 = TableReference.createWithEmptyNamespace("Table2");
    private static final String KVS_METHOD_NAME_1 = "getRows";
    private static final String KVS_METHOD_NAME_2 = "getAsync";
    private static final String KVS_METHOD_NAME_3 = "get";

    private static final long NO_BYTES_READ = 0L;
    private static final long BYTES_READ_1 = 10L;
    private static final long BYTES_READ_2 = 64L;
    private static final long BYTES_READ_3 = 300L;

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
        tracker.readForTable(TABLE_1, KVS_METHOD_NAME_1, BYTES_READ_2);
        tracker.readForTable(TABLE_1, KVS_METHOD_NAME_2, NO_BYTES_READ);

        tracker.readForTable(TABLE_2, KVS_METHOD_NAME_2, BYTES_READ_2);
        tracker.readForTable(TABLE_2, KVS_METHOD_NAME_3, BYTES_READ_3);

        tracker.readForTable(TABLE_3, KVS_METHOD_NAME_3, BYTES_READ_3);
        tracker.readForTable(TABLE_3, KVS_METHOD_NAME_1, BYTES_READ_1);
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
    public void oneCallForTableIsTracked() {
        tracker.callForTable(TABLE_2);
        TransactionReadInfo readInfo =
                ImmutableTransactionReadInfo.builder().bytesRead(0).kvsCalls(1).build();
        assertEquals(readInfo, tracker.getReadInfo());
        assertEquals(ImmutableMap.of(), tracker.getReadInfoByTable());
    }

    @Test
    public void oneTableAgnosticReadIsTracked() {
        tracker.tableAgnosticRead(KVS_METHOD_NAME_3, BYTES_READ_3);
        TransactionReadInfo readInfo = ImmutableTransactionReadInfo.builder()
                .bytesRead(BYTES_READ_3)
                .maximumBytesKvsCallInfo(ImmutableKvsCallReadInfo.builder()
                        .bytesRead(BYTES_READ_3)
                        .methodName(KVS_METHOD_NAME_3)
                        .build())
                .kvsCalls(1)
                .build();
        assertEquals(readInfo, tracker.getReadInfo());
        assertEquals(ImmutableMap.of(), tracker.getReadInfoByTable());
    }
}
