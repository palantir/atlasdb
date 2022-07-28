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

public enum V1EncodingStrategy implements TimestampEncodingStrategy<TransactionStatus> {
    INSTANCE;

    @Override
    public Cell encodeStartTimestampAsCell(long startTimestamp) {
        return Cell.create(
                TransactionConstants.getValueForTimestamp(startTimestamp), TransactionConstants.COMMIT_TS_COLUMN);
    }

    @Override
    public long decodeCellAsStartTimestamp(Cell cell) {
        return TransactionConstants.getTimestampForValue(cell.getRowName());
    }

    @Override
    public byte[] encodeCommitTimestampAsValue(long _ignoredStartTimestamp, TransactionStatus transactionStatus) {
        return TransactionStatuses.caseOf(transactionStatus)
                .committed(TransactionConstants::getValueForTimestamp)
                .otherwise_(PtBytes.EMPTY_BYTE_ARRAY);
    }

    @Override
    public TransactionStatus decodeValueAsCommitTimestamp(long _ignoredStartTimestamp, byte[] value) {
        if (value == null) {
            return TransactionConstants.IN_PROGRESS;
        }
        return TransactionStatuses.committed(TransactionConstants.getTimestampForValue(value));
    }
}
