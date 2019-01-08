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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.CheckForNull;

import org.immutables.value.Value;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

/**
 * This class coalesces write (that is, put-unless-exists) requests to an underlying {@link TransactionService}, such
 * that there is at most one request in flight at a given time. Read requests (gets) are not batched.
 */
public class WriteBatchingTransactionService implements TransactionService, AutoCloseable {
    private final TransactionService delegate;
    private final DisruptorAutobatcher<TimestampPair, Void> autobatcher;

    private WriteBatchingTransactionService(
            TransactionService delegate, DisruptorAutobatcher<TimestampPair, Void> autobatcher) {
        this.delegate = delegate;
        this.autobatcher = autobatcher;
    }

    public static TransactionService create(TransactionService delegate) {
        DisruptorAutobatcher<TimestampPair, Void> autobatcher = DisruptorAutobatcher.create(
                elements -> processBatch(delegate, elements));
        return new WriteBatchingTransactionService(delegate, autobatcher);
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
        try {
            autobatcher.apply(TimestampPair.of(startTimestamp, commitTimestamp)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        autobatcher.close();
    }

    private static void processBatch(
            TransactionService delegate, List<BatchElement<TimestampPair, Void>> batchElements) {
        Map<Long, Long> accumulatedRequest = Maps.newHashMapWithExpectedSize(batchElements.size());
        for (BatchElement<TimestampPair, Void> batchElement : batchElements) {
            TimestampPair pair = batchElement.argument();
            accumulatedRequest.merge(pair.startTimestamp(), pair.commitTimestamp(), (oldValue, newValue) -> {
                batchElement.result().setException(
                        new SafeIllegalArgumentException("Attempted to putUnlessExists the same start timestamp as"
                                + " another request which came first, so we will prioritise that.",
                                SafeArg.of("startTimestamp", pair.startTimestamp()),
                                SafeArg.of("commitTimestamp", pair.commitTimestamp()),
                                SafeArg.of("theirTimestamp", oldValue)));
                return oldValue;
            });
        }
        delegate.putUnlessExistsMultiple(accumulatedRequest);
    }

    @Value.Immutable
    interface TimestampPair {
        @Value.Parameter
        long startTimestamp();

        @Value.Parameter
        long commitTimestamp();

        static TimestampPair of(long startTimestamp, long commitTimestamp) {
            return ImmutableTimestampPair.of(startTimestamp, commitTimestamp);
        }
    }
}
