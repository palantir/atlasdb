/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.pue.PutUnlessExistsValue;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Arrays;
import java.util.stream.Stream;

public enum TwoPhaseEncodingStrategy implements TimestampEncodingStrategy<PutUnlessExistsValue<TransactionStatus>> {
    INSTANCE;

    private static final byte[] STAGING = new byte[] {0};
    private static final byte[] COMMITTED = new byte[] {1};

    public static final byte[] ABORTED_TRANSACTION_COMMITTED_VALUE =
            EncodingUtils.add(TransactionConstants.ABORTED_TRANSACTION_VALUE, COMMITTED);
    private static final PutUnlessExistsValue<TransactionStatus> IN_PROGRESS =
            PutUnlessExistsValue.committed(TransactionConstants.IN_PROGRESS);

    @Override
    public Cell encodeStartTimestampAsCell(long startTimestamp) {
        return TicketsEncodingStrategy.INSTANCE.encodeStartTimestampAsCell(startTimestamp);
    }

    @Override
    public long decodeCellAsStartTimestamp(Cell cell) {
        return TicketsEncodingStrategy.INSTANCE.decodeCellAsStartTimestamp(cell);
    }

    @Override
    public byte[] encodeCommitTimestampAsValue(
            long startTimestamp, PutUnlessExistsValue<TransactionStatus> commitTimestamp) {
        return EncodingUtils.add(
                TicketsEncodingStrategy.INSTANCE.encodeCommitTimestampAsValue(startTimestamp, commitTimestamp.value()),
                commitTimestamp.isCommitted() ? COMMITTED : STAGING);
    }

    @Override
    public PutUnlessExistsValue<TransactionStatus> decodeValueAsCommitTimestamp(long startTimestamp, byte[] value) {
        if (value == null) {
            return IN_PROGRESS;
        }

        byte[] head = PtBytes.head(value, value.length - 1);
        byte[] tail = PtBytes.tail(value, 1);

        TransactionStatus commitStatus =
                TicketsEncodingStrategy.INSTANCE.decodeValueAsCommitTimestamp(startTimestamp, head);
        if (Arrays.equals(tail, COMMITTED)) {
            return PutUnlessExistsValue.committed(commitStatus);
        }
        if (Arrays.equals(tail, STAGING)) {
            return PutUnlessExistsValue.staging(commitStatus);
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
