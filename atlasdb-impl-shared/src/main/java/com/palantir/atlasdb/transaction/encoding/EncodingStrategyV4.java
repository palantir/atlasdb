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

package com.palantir.atlasdb.transaction.encoding;

import com.palantir.atlasdb.atomic.AtomicValue;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import java.util.Arrays;

public enum EncodingStrategyV4 implements TimestampEncodingStrategy<AtomicValue<TransactionStatus>> {
    INSTANCE;

    private static final byte[] TTS_IN_PROGRESS_TRANSACTION_VALUE = new byte[] {0};

    @Override
    public Cell encodeStartTimestampAsCell(long startTimestamp) {
        return TicketsEncodingStrategy.INSTANCE.encodeStartTimestampAsCell(startTimestamp);
    }

    @Override
    public long decodeCellAsStartTimestamp(Cell cell) {
        return TicketsEncodingStrategy.INSTANCE.decodeCellAsStartTimestamp(cell);
    }

    @Override
    public byte[] encodeCommitStatusAsValue(long startTimestamp, AtomicValue<TransactionStatus> commitTimestamp) {
        return TwoPhaseEncodingStrategy.INSTANCE.encodeCommitTimestampAsValue(startTimestamp, commitTimestamp);
    }

    @Override
    public AtomicValue<TransactionStatus> decodeValueAsCommitStatus(long startTimestamp, byte[] value) {
        return Arrays.equals(value, TTS_IN_PROGRESS_TRANSACTION_VALUE)
                ? TwoPhaseEncodingStrategy.IN_PROGRESS_COMMITTED
                : TwoPhaseEncodingStrategy.INSTANCE.decodeValueAsTransactionStatus(startTimestamp, value);
    }
}
