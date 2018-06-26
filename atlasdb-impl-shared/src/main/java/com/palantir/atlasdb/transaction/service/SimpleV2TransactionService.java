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

package com.palantir.atlasdb.transaction.service;

import org.apache.commons.lang3.ArrayUtils;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;

public class SimpleV2TransactionService extends AbstractKeyValueServiceBackedTransactionService {
    private static final long PARTITIONING_QUANTUM = 100_000_000;
    private static final long ROWS_PER_QUANTUM = 256;

    private final KeyValueService keyValueService;

    public SimpleV2TransactionService(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    @Override
    public KeyValueService getKeyValueService() {
        return keyValueService;
    }

    @Override
    public TableReference getTableReference() {
        return TransactionConstants.TRANSACTION_TABLE_V2;
    }

    @Override
    public Cell encodeTimestampAsCell(long startTimestamp) {
        // A long is 9 bytes at most; four of them makes 36 bytes.
        // If we have 256 rows per 100M, then one is safely below 1M dynamic column keys, and a row is bounded at 14M.
        long row = (startTimestamp / PARTITIONING_QUANTUM) * ROWS_PER_QUANTUM
                + (startTimestamp % PARTITIONING_QUANTUM) % ROWS_PER_QUANTUM;
        long col = (startTimestamp % PARTITIONING_QUANTUM) / ROWS_PER_QUANTUM;

        // Compression, but we want reverse order
        byte[] rowBytes = ValueType.VAR_LONG.convertFromJava(row);
        ArrayUtils.reverse(rowBytes);

        byte[] colBytes = ValueType.VAR_LONG.convertFromJava(col);
        return Cell.create(rowBytes, colBytes);
    }

    @Override
    public long decodeCellAsTimestamp(Cell cell) {
        byte[] rowBytes = cell.getRowName();
        ArrayUtils.reverse(rowBytes);
        long rowComponent = (Long) ValueType.VAR_LONG.convertToJava(rowBytes, 0);

        byte[] colBytes = cell.getColumnName();
        long colComponent = (Long) ValueType.VAR_LONG.convertToJava(colBytes, 0);

        return (rowComponent / ROWS_PER_QUANTUM) * PARTITIONING_QUANTUM
                + colComponent * ROWS_PER_QUANTUM
                + rowComponent % ROWS_PER_QUANTUM;
    }

    @Override
    public byte[] encodeTimestampAsValue(long startTimestamp) {
        return TransactionConstants.getValueForTimestamp(startTimestamp);
    }

    @Override
    public long decodeValueAsTimestamp(byte[] value) {
        return TransactionConstants.getTimestampForValue(value);
    }

    public static void main(String[] args) {
        SimpleV2TransactionService tx = new SimpleV2TransactionService(new InMemoryKeyValueService(false));
        System.out.println(tx.encodeTimestampAsCell(1));
        System.out.println(tx.encodeTimestampAsCell(PARTITIONING_QUANTUM + 1));
        System.out.println(tx.encodeTimestampAsCell(1000000001));
    }
}
