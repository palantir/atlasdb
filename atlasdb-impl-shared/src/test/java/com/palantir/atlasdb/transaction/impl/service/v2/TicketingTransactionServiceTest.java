/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.impl.service.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.v2.TicketingTransactionService;

public class TicketingTransactionServiceTest {
    private static final long START_TS_1 = 7;
    private static final long START_TS_2 = 77;
    private static final long COMMIT_TS_1 = 42;
    private static final long COMMIT_TS_2 = 422;

    private KeyValueService kvs;
    private TicketingTransactionService ticketingTransactionService;

    @Before
    public void setUp() {
        kvs = new InMemoryKeyValueService(true);
        ticketingTransactionService = new TicketingTransactionService(kvs);
    }

    @Test
    public void returnsNullIfCommitTimestampNotKnown() {
        assertThat(ticketingTransactionService.get(START_TS_1)).isNull();
    }

    @Test
    public void returnsCommitTimestampIfStartTimestampKnown() {
        ticketingTransactionService.putUnlessExists(START_TS_1, COMMIT_TS_1);
        assertThat(ticketingTransactionService.get(START_TS_1)).isEqualTo(COMMIT_TS_1);
    }

    @Test
    public void queriesAreDoneByStartTimestamp() {
        ticketingTransactionService.putUnlessExists(START_TS_1, COMMIT_TS_1);
        assertThat(ticketingTransactionService.get(COMMIT_TS_1)).isNull();
    }

    @Test
    public void canPutMultipleTimestamps() {
        ticketingTransactionService.putUnlessExists(START_TS_1, COMMIT_TS_1);
        ticketingTransactionService.putUnlessExists(START_TS_2, COMMIT_TS_2);
        assertThat(ticketingTransactionService.get(ImmutableList.of(START_TS_1, START_TS_2)))
                .containsEntry(START_TS_1, COMMIT_TS_1)
                .containsEntry(START_TS_2, COMMIT_TS_2);
    }

    @Test
    public void getMultipleTimestampsDoesNotContainUncommittedStartTimestamps() {
        ticketingTransactionService.putUnlessExists(START_TS_1, COMMIT_TS_1);
        assertThat(ticketingTransactionService.get(ImmutableList.of(START_TS_1, START_TS_2)))
                .containsEntry(START_TS_1, COMMIT_TS_1)
                .doesNotContainKey(START_TS_2);
    }

    @Test
    public void cannotReuseTimestamps() {
        ticketingTransactionService.putUnlessExists(START_TS_1, COMMIT_TS_1);
        assertThatThrownBy(() -> ticketingTransactionService.putUnlessExists(START_TS_1, COMMIT_TS_2))
                .isInstanceOf(KeyAlreadyExistsException.class);

        assertThat(ticketingTransactionService.get(START_TS_1)).isEqualTo(COMMIT_TS_1);
    }

    @Test
    public void cannotReuseTimestampsEvenIfCommitTimestampMatches() {
        ticketingTransactionService.putUnlessExists(START_TS_1, COMMIT_TS_1);
        assertThatThrownBy(() -> ticketingTransactionService.putUnlessExists(START_TS_1, COMMIT_TS_1))
                .isInstanceOf(KeyAlreadyExistsException.class);

        assertThat(ticketingTransactionService.get(START_TS_1)).isEqualTo(COMMIT_TS_1);
    }

    @Test
    public void canRetrieveTimestampsNearRowBoundary() {
        long numRows = TicketingTransactionService.ROWS_PER_QUANTUM;
        ensureTimestampsCanBeDistinguished(numRows - 1, numRows, numRows + 1);
    }

    @Test
    public void canRetrieveTimestampsAcrossQuantumBoundary() {
        long quantum = TicketingTransactionService.PARTITIONING_QUANTUM;
        ensureTimestampsCanBeDistinguished(quantum - 1, quantum, quantum + 1);
    }

    @Test
    public void canDistinguishTimestampsAcrossRows() {
        long numRows = TicketingTransactionService.ROWS_PER_QUANTUM;
        long offset = 318557;
        ensureTimestampsCanBeDistinguished(offset, numRows + offset, 2 * numRows + offset);
    }

    @Test
    public void canDistinguishTimestampsAcrossQuanta() {
        long quantum = TicketingTransactionService.PARTITIONING_QUANTUM;
        long offset = 318557;
        ensureTimestampsCanBeDistinguished(offset, quantum + offset, 2 * quantum + offset);
    }

    @Test
    public void canDistinguishTimestampsAcrossExtremesOfQuanta() {
        long quantum = TicketingTransactionService.PARTITIONING_QUANTUM;
        ensureTimestampsCanBeDistinguished(
                quantum - 1, quantum, quantum + 1, 2 * quantum - 1, 2 * quantum, 2 * quantum + 1);
    }

    @Test
    public void storesLargeCommitTimestampsCompactly() {
        long highTimestamp = Long.MAX_VALUE - 1;
        ticketingTransactionService.putUnlessExists(highTimestamp, highTimestamp + 1);

        List<RowResult<Value>> values =
                kvs.getRange(TransactionConstants.TRANSACTION_TABLE_V2, RangeRequest.all(), Long.MAX_VALUE)
                        .stream()
                        .collect(Collectors.toList());
        byte[] data = Iterables.getOnlyElement(values).getOnlyColumnValue().getContents();
        assertThat(data.length).isEqualTo(1);
        assertThat(ticketingTransactionService.decodeValueAsCommitTimestamp(highTimestamp, data))
                .isEqualTo(highTimestamp + 1);
    }

    @Test
    public void distributesTimestampsAcrossRows() {
        for (int timestamp = 0; timestamp < TicketingTransactionService.ROWS_PER_QUANTUM; timestamp++) {
            ticketingTransactionService.putUnlessExists(
                    timestamp, timestamp + TicketingTransactionService.ROWS_PER_QUANTUM);
        }

        List<RowResult<Value>> values =
                kvs.getRange(TransactionConstants.TRANSACTION_TABLE_V2, RangeRequest.all(), Long.MAX_VALUE)
                        .stream()
                        .collect(Collectors.toList());

        // each row is represented
        assertThat(values.size()).isEqualTo(TicketingTransactionService.ROWS_PER_QUANTUM);
    }

    @Test
    public void distributesTimestampsEvenlyAcrossRows() {
        int elementsExpectedPerRow = 13;
        int numTimestamps = TicketingTransactionService.ROWS_PER_QUANTUM * elementsExpectedPerRow;
        int numPartitions = 10;

        for (int partition = 0; partition < numPartitions; partition++) {
            for (int timestamp = 0; timestamp < numTimestamps; timestamp++) {
                ticketingTransactionService.putUnlessExists(
                        TicketingTransactionService.PARTITIONING_QUANTUM * partition + timestamp,
                        TicketingTransactionService.PARTITIONING_QUANTUM * partition + timestamp + 5);
            }
        }

        List<Integer> rowSizes = kvs.getRange(
                TransactionConstants.TRANSACTION_TABLE_V2, RangeRequest.all(), Long.MAX_VALUE)
                .stream()
                .map(rowResult -> rowResult.getColumns().size())
                .collect(Collectors.toList());

        assertThat(rowSizes)
                .hasSize(TicketingTransactionService.ROWS_PER_QUANTUM * numPartitions)
                .allMatch(size -> size == elementsExpectedPerRow);
    }

    @Test
    public void cellEncodeAndDecodeAreInverses() {
        fuzzOneThousandTrials(() -> {
            long timestamp = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
            Cell encoded = ticketingTransactionService.encodeStartTimestampAsCell(timestamp);
            assertThat(ticketingTransactionService.decodeCellAsStartTimestamp(encoded)).isEqualTo(timestamp);
        });
    }

    @Test
    public void commitTimestampEncodeAndDecodeAreInverses() {
        fuzzOneThousandTrials(() -> {
            long startTimestamp = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE - 1);
            long commitTimestamp = ThreadLocalRandom.current().nextLong(startTimestamp, Long.MAX_VALUE);
            byte[] encoded = ticketingTransactionService.encodeCommitTimestampAsValue(
                     startTimestamp, commitTimestamp);
            assertThat(ticketingTransactionService.decodeValueAsCommitTimestamp(startTimestamp, encoded))
                    .isEqualTo(commitTimestamp);
        });
    }

    private void fuzzOneThousandTrials(Runnable test) {
        IntStream.range(0, 1000)
                .forEach(unused -> test.run());
    }

    private void ensureTimestampsCanBeDistinguished(long... timestamps) {
        Map<Long, Long> startToCommitTs = Arrays.stream(timestamps)
                .boxed()
                .collect(Collectors.toMap(
                        timestamp -> timestamp,
                        timestamp -> timestamp));

        startToCommitTs.forEach((start, commit) -> ticketingTransactionService.putUnlessExists(start, commit));

        assertThat(ticketingTransactionService.get(startToCommitTs.keySet()))
                .isEqualTo(startToCommitTs);
    }
}
