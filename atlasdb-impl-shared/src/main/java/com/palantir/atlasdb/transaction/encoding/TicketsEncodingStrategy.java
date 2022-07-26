/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.encoding;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatuses;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * The ticketing algorithm distributes timestamps among rows and dynamic columns to avoid hot-spotting.
 *
 * For the row and column partitioning, please consult {@link TicketsCellEncodingStrategy} for more details.
 * We use a delta encoding for the commit timestamp as these differences are expected to be small.
 *
 * A long is 9 bytes at most, so one row here consists of at most 4 longs -> 36 bytes, and an individual row
 * is probabilistically below 1M dynamic column keys. A row is expected to be less than 14M (56M worst case).
 *
 * Note the usage of {@link PtBytes#EMPTY_BYTE_ARRAY} for transactions that were rolled back; this is a space
 * optimisation, as we would otherwise store a negative value which uses 9 bytes in a VAR_LONG.
 */
public enum TicketsEncodingStrategy implements TimestampEncodingStrategy<TransactionStatus> {
    INSTANCE;

    public static final byte[] ABORTED_TRANSACTION_VALUE = PtBytes.EMPTY_BYTE_ARRAY;

    // DO NOT change the following without a transactions table migration!
    public static final long PARTITIONING_QUANTUM = 25_000_000;
    public static final int ROWS_PER_QUANTUM = TransactionConstants.V2_TRANSACTION_NUM_PARTITIONS;

    private static final TicketsCellEncodingStrategy CELL_ENCODING_STRATEGY =
            new TicketsCellEncodingStrategy(PARTITIONING_QUANTUM, ROWS_PER_QUANTUM);

    @Override
    public Cell encodeStartTimestampAsCell(long startTimestamp) {
        return CELL_ENCODING_STRATEGY.encodeStartTimestampAsCell(startTimestamp);
    }

    @Override
    public long decodeCellAsStartTimestamp(Cell cell) {
        return CELL_ENCODING_STRATEGY.decodeCellAsStartTimestamp(cell);
    }

    @Override
    public byte[] encodeCommitTimestampAsValue(long startTimestamp, TransactionStatus commitTimestamp) {
        return TransactionStatuses.caseOf(commitTimestamp)
                .committed(ts -> TransactionConstants.getValueForTimestamp(ts - startTimestamp))
                .aborted(() -> ABORTED_TRANSACTION_VALUE)
                .otherwise(() -> {
                    throw new SafeIllegalArgumentException(
                            "Unexpected transaction status", SafeArg.of("status", commitTimestamp));
                });
    }

    @Override
    public TransactionStatus decodeValueAsCommitTimestamp(long startTimestamp, byte[] value) {
        if (Arrays.equals(value, ABORTED_TRANSACTION_VALUE)) {
            return TransactionConstants.ABORTED;
        }
        return TransactionStatuses.committed(startTimestamp + TransactionConstants.getTimestampForValue(value));
    }

    public Stream<byte[]> getRowSetCoveringTimestampRange(long fromInclusive, long toInclusive) {
        return CELL_ENCODING_STRATEGY.getRowSetCoveringTimestampRange(fromInclusive, toInclusive);
    }
}
