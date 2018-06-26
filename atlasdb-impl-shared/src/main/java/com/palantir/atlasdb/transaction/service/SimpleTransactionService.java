/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;

public final class SimpleTransactionService extends AbstractKeyValueServiceBackedTransactionService {
    private final KeyValueService keyValueService;

    public SimpleTransactionService(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    // The maximum key-value store timestamp (exclusive) at which data is stored
    // in transaction table.
    // All entries in transaction table are stored with timestamp 0
    private static final long MAX_TIMESTAMP = 1L;

    @Override
    public KeyValueService getKeyValueService() {
        return keyValueService;
    }

    @Override
    public TableReference getTableReference() {
        return TransactionConstants.TRANSACTION_TABLE;
    }

    @Override
    public Cell encodeTimestampAsCell(long startTimestamp) {
        return getTransactionCell(startTimestamp);
    }

    @Override
    public long decodeCellAsTimestamp(Cell cell) {
        return TransactionConstants.getTimestampForValue(cell.getRowName());
    }

    @Override
    public byte[] encodeTimestampAsValue(long startTimestamp) {
        return TransactionConstants.getValueForTimestamp(startTimestamp);
    }

    @Override
    public long decodeValueAsTimestamp(byte[] value) {
        return TransactionConstants.getTimestampForValue(value);
    }

    private Cell getTransactionCell(long startTimestamp) {
        return Cell.create(
                TransactionConstants.getValueForTimestamp(startTimestamp),
                TransactionConstants.COMMIT_TS_COLUMN);
    }
}
