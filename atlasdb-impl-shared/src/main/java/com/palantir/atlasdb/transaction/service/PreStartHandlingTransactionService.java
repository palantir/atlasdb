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
import com.palantir.atlasdb.AtlasDbConstants;
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
 * This service handles queries for timestamps before {@link AtlasDbConstants#STARTING_TS}
 * as follows:
 *
 * - Gets of timestamps before {@link AtlasDbConstants#STARTING_TS} will return
 *   {@link AtlasDbConstants#STARTING_TS - 1}; in an AtlasDB context these correspond to
 *   deletion sentinels that are written non-transactionally and thus always committed.
 * - putUnlessExists to timestamps before {@link AtlasDbConstants#STARTING_TS} will throw an
 *   exception.
 *
 * Queries for legitimate timestamps are routed to the delegate.
 */
public class PreStartHandlingTransactionService implements TransactionService {
    private final InternalTransactionService delegate;
    private final InternalAsyncTransactionService synchronousAsyncTransactionService;

    PreStartHandlingTransactionService(InternalTransactionService delegate) {
        this.delegate = delegate;
        this.synchronousAsyncTransactionService =
                TransactionServices.synchronousAsInternalAsyncTransactionService(delegate);
    }

    @CheckForNull
    @Override
    public Long get(long startTimestamp) {
        return AtlasFutures.getUnchecked(getFromDelegate(startTimestamp, synchronousAsyncTransactionService));
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        return AtlasFutures.getUnchecked(getFromDelegate(startTimestamps, synchronousAsyncTransactionService));
    }

    @Override
    public ListenableFuture<Long> getAsync(long startTimestamp) {
        return getFromDelegate(startTimestamp, delegate);
    }

    @Override
    public ListenableFuture<Map<Long, Long>> getAsync(Iterable<Long> startTimestamps) {
        return getFromDelegate(startTimestamps, delegate);
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

    private ListenableFuture<Long> getFromDelegate(
            long startTimestamp, AsyncTransactionService asyncTransactionService) {
        return handlePreStart(startTimestamp, AtlasDbConstants.STARTING_TS - 1, asyncTransactionService::getAsync);
    }

    private ListenableFuture<Map<Long, Long>> getFromDelegate(
            Iterable<Long> startTimestamps, AsyncTransactionService asyncTransactionService) {
        return handlePreStart(startTimestamps, AtlasDbConstants.STARTING_TS - 1, asyncTransactionService::getAsync);
    }

    private ListenableFuture<TransactionStatus> getInternalFromDelegate(
            long startTimestamp, InternalAsyncTransactionService asyncTransactionService) {
        return handlePreStart(
                startTimestamp, TransactionConstants.PRE_START_COMMITTED, asyncTransactionService::getInternalAsync);
    }

    private ListenableFuture<Map<Long, TransactionStatus>> getInternalFromDelegate(
            Iterable<Long> startTimestamps, InternalAsyncTransactionService asyncTransactionService) {
        return handlePreStart(
                startTimestamps, TransactionConstants.PRE_START_COMMITTED, asyncTransactionService::getInternalAsync);
    }

    private <T> ListenableFuture<T> handlePreStart(
            long startTimestamp, T preStartCommit, Function<Long, ListenableFuture<T>> delegateCall) {
        if (!isTimestampValid(startTimestamp)) {
            return Futures.immediateFuture(preStartCommit);
        }
        return delegateCall.apply(startTimestamp);
    }

    private <T> ListenableFuture<Map<Long, T>> handlePreStart(
            Iterable<Long> startTimestamps,
            T preStartCommit,
            Function<Iterable<Long>, ListenableFuture<Map<Long, T>>> delegateCall) {
        Map<Boolean, List<Long>> classifiedTimestamps = StreamSupport.stream(startTimestamps.spliterator(), false)
                .collect(Collectors.partitioningBy(PreStartHandlingTransactionService::isTimestampValid));

        List<Long> validTimestamps = classifiedTimestamps.get(true);
        Map<Long, T> result = KeyedStream.of(classifiedTimestamps.get(false).stream())
                .map(_ignore -> preStartCommit)
                .collectTo(HashMap::new);

        if (!validTimestamps.isEmpty()) {
            return Futures.transform(
                    delegateCall.apply(validTimestamps),
                    timestampMap -> {
                        result.putAll(timestampMap);
                        return result;
                    },
                    MoreExecutors.directExecutor());
        }
        return Futures.immediateFuture(result);
    }

    private static boolean isTimestampValid(Long startTimestamp) {
        return startTimestamp >= AtlasDbConstants.STARTING_TS;
    }
}
