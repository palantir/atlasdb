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
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatuses;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Arrays;
import java.util.stream.Stream;

public final class TwoPhaseEncodingStrategy
        implements TransactionStatusEncodingStrategy<AtomicValue<TransactionStatus>> {
    private static final byte[] STAGING = new byte[] {0};
    private static final byte[] COMMITTED = new byte[] {1};

    private static final AtomicValue<TransactionStatus> IN_PROGRESS_COMMITTED =
            AtomicValue.committed(TransactionConstants.IN_PROGRESS);
    public static final byte[] ABORTED_TRANSACTION_COMMITTED_VALUE =
            EncodingUtils.add(TransactionConstants.TICKETS_ENCODING_ABORTED_TRANSACTION_VALUE, COMMITTED);
    public static final byte[] ABORTED_TRANSACTION_STAGING_VALUE =
            EncodingUtils.add(TransactionConstants.TICKETS_ENCODING_ABORTED_TRANSACTION_VALUE, STAGING);
    private final ProgressEncodingStrategy progressEncodingStrategy;

    public TwoPhaseEncodingStrategy(ProgressEncodingStrategy progressEncodingStrategy) {
        this.progressEncodingStrategy = progressEncodingStrategy;
    }

    @Override
    public Cell encodeStartTimestampAsCell(long startTimestamp) {
        return TicketsEncodingStrategy.INSTANCE.encodeStartTimestampAsCell(startTimestamp);
    }

    @Override
    public long decodeCellAsStartTimestamp(Cell cell) {
        return TicketsEncodingStrategy.INSTANCE.decodeCellAsStartTimestamp(cell);
    }

    @Override
    public byte[] encodeCommitStatusAsValue(long startTimestamp, AtomicValue<TransactionStatus> commitStatus) {
        return EncodingUtils.add(
                TicketsEncodingStrategy.INSTANCE.encodeCommitStatusAsValue(startTimestamp, commitStatus.value()),
                commitStatus.isCommitted() ? COMMITTED : STAGING);
    }

    public AtomicValue<TransactionStatus> decodeNullValueAsCommitStatus() {
        if (progressEncodingStrategy.isInProgress(null)) {
            return IN_PROGRESS_COMMITTED;
        }
        // `null` value is the in_progress marker for transaction schema <= 3. For transaction schema > 3, we have an
        // explicit in-progress marker and a null values represents that the table has been swept.
        return AtomicValue.committed(TransactionStatuses.unknown());
    }

    @Override
    public AtomicValue<TransactionStatus> decodeValueAsCommitStatus(long startTimestamp, byte[] value) {
        return progressEncodingStrategy.isInProgress(value)
                ? IN_PROGRESS_COMMITTED
                : decodeValueAsCommitStatusInternal(startTimestamp, value);
    }

    @Override
    public byte[] getInProgressMarker() {
        return progressEncodingStrategy.getInProgressMarker();
    }

    private AtomicValue<TransactionStatus> decodeValueAsCommitStatusInternal(long startTimestamp, byte[] value) {
        byte[] head = PtBytes.head(value, value.length - 1);
        byte[] tail = PtBytes.tail(value, 1);

        TransactionStatus commitStatus =
                TicketsEncodingStrategy.INSTANCE.decodeValueAsCommitStatus(startTimestamp, head);
        if (Arrays.equals(tail, COMMITTED)) {
            return AtomicValue.committed(commitStatus);
        }
        if (Arrays.equals(tail, STAGING)) {
            return AtomicValue.staging(commitStatus);
        }

        throw new SafeIllegalArgumentException("Unknown commit state.", SafeArg.of("bytes", Arrays.toString(tail)));
    }

    public Stream<byte[]> encodeRangeOfStartTimestampsAsRows(long fromInclusive, long toInclusive) {
        return TicketsEncodingStrategy.INSTANCE.getRowSetCoveringTimestampRange(fromInclusive, toInclusive);
    }

    public byte[] transformStagingToCommitted(byte[] stagingValue) {
        byte[] head = PtBytes.head(stagingValue, stagingValue.length - 1);
        byte[] tail = PtBytes.tail(stagingValue, 1);
        Preconditions.checkArgument(Arrays.equals(tail, STAGING), "Expected a staging value.");
        return EncodingUtils.add(head, COMMITTED);
    }
}
