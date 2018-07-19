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

package com.palantir.atlasdb.transaction.service.v2;

import org.apache.commons.lang3.ArrayUtils;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.AbstractKeyValueServiceBackedTransactionService;

/**
 * The ticketing algorithm distributes timestamps among rows and dynamic columns to avoid hot-spotting.
 *
 * We divide the first PARTITIONING_QUANTUM timestamps among the first ROW_PER_QUANTUM rows.
 * We aim to distribute start timestamps as evenly as possible among these rows as numbers increase, by taking the
 * least significant bits of the timestamp and using that as the row number. For example, we might store timestamps
 * 1, ROW_PER_QUANTUM + 1, 2 * ROW_PER_QUANTUM + 1 etc. in the same row.
 *
 * We store the row name as a little-endian representation of the row number to ensure even distribution in key-value
 * services that rely on consistent hashing or similar mechanisms for partitioning.
 *
 * We also use a delta encoding for the commit timestamp as these differences are expected to be small.
 *
 * A long is 9 bytes at most, so one row here consists of at most 4 longs -> 36 bytes, and an individual row
 * is probabilistically below 1M dynamic column keys. A row is expected to be about 6.5M (27M worst case).
 */
public class TicketingTransactionService extends AbstractKeyValueServiceBackedTransactionService {
    // DO NOT change the following without a transactions table migration!
    public static final long PARTITIONING_QUANTUM = 400_000_000;
    public static final int ROWS_PER_QUANTUM = 256;

    public TicketingTransactionService(KeyValueService keyValueService) {
        super(keyValueService);
    }

    @Override
    public TableReference getTableReference() {
        return TransactionConstants.TRANSACTION_TABLE_V2;
    }

    @Override
    public Cell encodeStartTimestampAsCell(long startTimestamp) {
        byte[] rowName = encodeRowName(startTimestamp);
        byte[] columnName = encodeColumnName(startTimestamp);
        return Cell.create(rowName, columnName);
    }

    @Override
    public long decodeCellAsStartTimestamp(Cell cell) {
        long rowComponent = decodeRowName(cell.getRowName());
        long columnComponent = decodeColumnName(cell.getColumnName());

        return (rowComponent / ROWS_PER_QUANTUM) * PARTITIONING_QUANTUM
                + columnComponent * ROWS_PER_QUANTUM
                + rowComponent % ROWS_PER_QUANTUM;
    }

    @Override
    public byte[] encodeCommitTimestampAsValue(long startTimestamp, long commitTimestamp) {
        return TransactionConstants.getValueForTimestamp(commitTimestamp - startTimestamp);
    }

    @Override
    public long decodeValueAsCommitTimestamp(long startTimestamp, byte[] value) {
        return startTimestamp + TransactionConstants.getTimestampForValue(value);
    }

    private byte[] encodeRowName(long startTimestamp) {
        long row = (startTimestamp / PARTITIONING_QUANTUM) * ROWS_PER_QUANTUM
                + (startTimestamp % PARTITIONING_QUANTUM) % ROWS_PER_QUANTUM;
        byte[] rowName = ValueType.VAR_LONG.convertFromJava(row);
        ArrayUtils.reverse(rowName);
        return rowName;
    }

    private byte[] encodeColumnName(long startTimestamp) {
        long column = (startTimestamp % PARTITIONING_QUANTUM) / ROWS_PER_QUANTUM;
        return ValueType.VAR_LONG.convertFromJava(column);
    }

    private long decodeRowName(byte[] rowName) {
        ArrayUtils.reverse(rowName);
        return (long) ValueType.VAR_LONG.convertToJava(rowName, 0);
    }

    private long decodeColumnName(byte[] columnName) {
        return (long) ValueType.VAR_LONG.convertToJava(columnName, 0);
    }
}
