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

package com.palantir.atlasdb.transaction.service;

import java.util.Map;
import java.util.function.LongSupplier;

import javax.annotation.CheckForNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

/**
 * A {@link TransactionService} that also limits the gap between start and commit timestamps to be not more than
 * a provided limit (that may change dynamically), throwing if the difference between the two exceeds this.
 */
public class GapLimitingTransactionService implements TransactionService {
    private final TransactionService delegate;
    private final LongSupplier limitSupplier;

    @VisibleForTesting
    GapLimitingTransactionService(TransactionService delegate, LongSupplier limitSupplier) {
        this.delegate = delegate;
        this.limitSupplier = limitSupplier;
    }

    /**
     * Creates a {@link TransactionService} intended for use with _transactions2 and its
     * {@link com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy}. More concretely, this enforces
     * that the commit timestamp is no more than {@link TransactionConstants#V2_PARTITIONING_QUANTUM} ahead of
     * the start timestamp; this is useful for workflows where one needs to range scan the values of the
     * _transactions2 table (e.g. as part of a restore).
     *
     * @param delegate underlying transaction service
     * @return a {@link TransactionService} that routes calls to the delegates and enforces a gap equal to a quantum.
     */
    public static TransactionService createDefaultForV2(TransactionService delegate) {
        return new GapLimitingTransactionService(delegate, () -> TransactionConstants.V2_PARTITIONING_QUANTUM);
    }

    @CheckForNull
    @Override
    public Long get(long startTimestamp) {
        return delegate.get(startTimestamp);
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        return delegate.get(startTimestamps);
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        long limit = getAndVerifyLimitIsPositive();
        if (!startAndCommitTimestampsComplyWithLimit(startTimestamp, commitTimestamp, limit)) {
            throw new SafeIllegalStateException("Timestamp gap is too large! Expected a gap no larger than"
                    + " {}, but observed a larger gap - (start: {}, commit: {})",
                    SafeArg.of("gapLimit", limit),
                    SafeArg.of("startTimestamp", startTimestamp),
                    SafeArg.of("commitTimestamp", commitTimestamp));
        }
        delegate.putUnlessExists(startTimestamp, commitTimestamp);

    }

    @Override
    public void putUnlessExistsMultiple(Map<Long, Long> startTimestampToCommitTimestamp) {
        long limit = getAndVerifyLimitIsPositive();
        Map<Long, Long> legitimatePairs = Maps.filterEntries(startTimestampToCommitTimestamp,
                entry -> startAndCommitTimestampsComplyWithLimit(entry.getKey(), entry.getValue(), limit));
        if (legitimatePairs.size() != startTimestampToCommitTimestamp.size()) {
            Map<Long, Long> illegitimatePairs = Maps.difference(startTimestampToCommitTimestamp, legitimatePairs)
                    .entriesOnlyOnLeft();
            throw new SafeIllegalStateException("Timestamp gap is too large for some pairs. Expected a gap no"
                    + " larger than {}, but observed the following start-commit timestamp pairs where this gap was"
                    + " exceeded: {}",
                    SafeArg.of("gapLimit", limit),
                    SafeArg.of("illegitimatePairs", illegitimatePairs));
        }
        delegate.putUnlessExistsMultiple(legitimatePairs);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private boolean startAndCommitTimestampsComplyWithLimit(long startTimestamp, long commitTimestamp, long limit) {
        // Safe to perform naive subtraction, as commitTimestamp >= -1 and startTimestamp >= 1
        return commitTimestamp - startTimestamp <= limit;
    }

    private long getAndVerifyLimitIsPositive() {
        long limit = limitSupplier.getAsLong();
        if (limit <= 0) {
            throw new SafeIllegalStateException("Gap limiting transaction service was provided a limit of {},"
                    + " which is unreasonable as the commit timestamp is always greater than the start timestamp,"
                    + " hence limits less than 1 are unacceptable.", SafeArg.of("gapLimit", limit));
        }
        return limit;
    }
}
