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

import com.google.common.collect.Maps;
import com.google.common.math.LongMath;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

/**
 * A {@link TransactionService} that also limits the
 */
public class GapLimitingTransactionService implements TransactionService {
    private final TransactionService delegate;
    private final LongSupplier limitSupplier;

    private GapLimitingTransactionService(TransactionService delegate, LongSupplier limitSupplier) {
        this.delegate = delegate;
        this.limitSupplier = limitSupplier;
    }

    public TransactionService createDefaultForV2(TransactionService delegate) {
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
        long observedGap = LongMath.saturatedSubtract(commitTimestamp, startTimestamp);
        long limit = limitSupplier.getAsLong();
        if (observedGap > limit) {
            throw new SafeIllegalStateException("Timestamp gap is too large! Expected a gap no larger than"
                    + " {}, but observed a gap of {} (start: {}, commit: {})",
                    SafeArg.of("gapLimit", limit),
                    SafeArg.of("observedGap", observedGap),
                    SafeArg.of("startTimestamp", startTimestamp),
                    SafeArg.of("commitTimestamp", commitTimestamp));
        }
        delegate.putUnlessExists(startTimestamp, commitTimestamp);

    }

    @Override
    public void putUnlessExistsMultiple(Map<Long, Long> startTimestampToCommitTimestamp) {
        long limit = limitSupplier.getAsLong();
        Map<Long, Long> legitimatePairs = Maps.filterEntries(startTimestampToCommitTimestamp,
                entry -> LongMath.saturatedSubtract(entry.getValue(), entry.getKey()) <= limit);
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
}
