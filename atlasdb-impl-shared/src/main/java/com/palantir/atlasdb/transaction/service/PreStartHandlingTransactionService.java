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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.CheckForNull;

/**
 * This service handles queries for timestamps before {@link TransactionConstants#LOWEST_POSSIBLE_START_TS}
 * as follows:
 *
 * - Gets of timestamps before {@link TransactionConstants#LOWEST_POSSIBLE_START_TS} will return
 *   {@link TransactionConstants#PRE_START_COMMITTED}; in an AtlasDB context these correspond to
 *   deletion sentinels that are written non-transactionally and thus always committed.
 * - putUnlessExists to timestamps before {@link TransactionConstants#PRE_START_COMMITTED} will throw an
 *   exception.
 *
 * Queries for legitimate timestamps are routed to the delegate.
 */
public class PreStartHandlingTransactionService implements TransactionService {
    private final TransactionService delegate;
    private final AsyncTransactionService synchronousAsyncTransactionService;

    PreStartHandlingTransactionService(TransactionService delegate) {
        this.delegate = delegate;
        this.synchronousAsyncTransactionService = TransactionServices.synchronousAsAsyncTransactionService(delegate);
    }

    @CheckForNull
    @Override
    public Long get(long startTimestamp) {
        return AtlasFutures.getUnchecked(getFromDelegate(
                startTimestamp,
                synchronousAsyncTransactionService::getAsync,
                TransactionConstants.PRE_START_COMMITTED_TS));
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        return AtlasFutures.getUnchecked(getFromDelegate(
                startTimestamps,
                synchronousAsyncTransactionService::getAsync,
                TransactionConstants.PRE_START_COMMITTED_TS));
    }

    @Override
    public void markInProgress(long startTimestamp) {
        delegate.markInProgress(startTimestamp);
    }

    @Override
    public ListenableFuture<Long> getAsync(long startTimestamp) {
        return getFromDelegate(startTimestamp, delegate::getAsync, TransactionConstants.PRE_START_COMMITTED_TS);
    }

    @Override
    public ListenableFuture<Map<Long, Long>> getAsync(Iterable<Long> startTimestamps) {
        return getFromDelegate(startTimestamps, delegate::getAsync, TransactionConstants.PRE_START_COMMITTED_TS);
    }

    @Override
    public ListenableFuture<TransactionStatus> getAsyncV2(long startTimestamp) {
        return getFromDelegate(startTimestamp, delegate::getAsyncV2, TransactionConstants.PRE_START_COMMITTED);
    }

    @Override
    public ListenableFuture<Map<Long, TransactionStatus>> getAsyncV2(Iterable<Long> startTimestamps) {
        return getFromDelegate(startTimestamps, delegate::getAsyncV2, TransactionConstants.PRE_START_COMMITTED);
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        if (!isTimestampValid(startTimestamp)) {
            throw new SafeIllegalStateException(
                    "Attempted to putUnlessExists from an invalid start timestamp, which is disallowed.",
                    SafeArg.of("startTimestamp", startTimestamp),
                    SafeArg.of("commitTimestamp", commitTimestamp));
        }
        delegate.putUnlessExists(startTimestamp, commitTimestamp);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private <T> ListenableFuture<T> getFromDelegate(
            long startTimestamp, Function<Long, ListenableFuture<T>> getter, T invalidCommit) {
        if (!isTimestampValid(startTimestamp)) {
            return Futures.immediateFuture(invalidCommit);
        }
        return getter.apply(startTimestamp);
    }

    private <T> ListenableFuture<Map<Long, T>> getFromDelegate(
            Iterable<Long> startTimestamps,
            Function<Iterable<Long>, ListenableFuture<Map<Long, T>>> getter,
            T invalidCommit) {
        Map<Boolean, List<Long>> classifiedTimestamps = StreamSupport.stream(startTimestamps.spliterator(), false)
                .collect(Collectors.partitioningBy(PreStartHandlingTransactionService::isTimestampValid));

        Map<Long, T> result = KeyedStream.of(classifiedTimestamps.get(false).stream())
                .map(_ignore -> invalidCommit)
                .collectTo(HashMap::new);

        List<Long> validTimestamps = classifiedTimestamps.get(true);
        if (!validTimestamps.isEmpty()) {
            return Futures.transform(
                    getter.apply(validTimestamps),
                    timestampMap -> {
                        result.putAll(timestampMap);
                        return result;
                    },
                    MoreExecutors.directExecutor());
        }
        return Futures.immediateFuture(result);
    }

    private static boolean isTimestampValid(Long startTimestamp) {
        return startTimestamp >= TransactionConstants.LOWEST_POSSIBLE_START_TS;
    }
}
