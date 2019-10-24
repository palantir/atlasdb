/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.CheckForNull;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

/**
 * A {@link SplitKeyDelegatingTransactionService} delegates between multiple {@link TransactionService}s, depending
 * on which timestamps are requested. This class preserves the {@link TransactionService} guarantees regardless of
 * which underlying service is contacted.
 *
 * The timestampToServiceKey function is expected to handle all timestamps greater than or equal to
 * {@link com.palantir.atlasdb.AtlasDbConstants#STARTING_TS}. It may, but is not expected to, handle timestamps
 * below that. The function may return null; if it does, then for reads, values written at that timestamp are
 * considered to be uncommitted. The transaction service will throw if a write is attempted at such a timestamp.
 *
 * The transaction service will also throw an exception if the timestamp-to-service-key function returns a key which is
 * not in the keyedServices map.
 *
 * Service keys are expected to be safe for logging.
 */
public class SplitKeyDelegatingTransactionService<T> implements TransactionService {
    private final Function<Long, T> timestampToServiceKey;
    private final Map<T, TransactionService> keyedServices;
    private final TimestampLoader immediateTimestampLoader;

    public SplitKeyDelegatingTransactionService(
            Function<Long, T> timestampToServiceKey,
            Map<T, TransactionService> keyedServices) {
        this.timestampToServiceKey = timestampToServiceKey;
        this.keyedServices = keyedServices;
        this.immediateTimestampLoader = new TimestampLoader() {
            @Override
            public ListenableFuture<Long> get(TransactionService transactionService, long startTimestamp) {
                return Futures.immediateFuture(transactionService.get(startTimestamp));
            }

            @Override
            public ListenableFuture<Map<Long, Long>> get(
                    TransactionService transactionService,
                    Iterable<Long> startTimestamps) {
                return Futures.immediateFuture(transactionService.get(startTimestamps));
            }
        };
    }

    @CheckForNull
    @Override
    public Long get(long startTimestamp) {
        return AtlasFutures.runWithException(() -> getInternal(startTimestamp, immediateTimestampLoader));
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        return AtlasFutures.runWithException(() -> getInternal(startTimestamps, immediateTimestampLoader));
    }

    private ListenableFuture<Long> getInternal(long startTimestamp, TimestampLoader cellLoader) {
        return getServiceForTimestamp(startTimestamp)
                .map(service -> cellLoader.get(service, startTimestamp))
                .orElseGet(() -> Futures.immediateFuture(null));
    }

    private ListenableFuture<Map<Long, Long>> getInternal(Iterable<Long> startTimestamps, TimestampLoader cellLoader) {
        Multimap<T, Long> queryMap = HashMultimap.create();
        for (Long startTimestamp : startTimestamps) {
            T mappedValue = timestampToServiceKey.apply(startTimestamp);
            if (mappedValue != null) {
                queryMap.put(mappedValue, startTimestamp);
            }
        }

        Set<T> unknownKeys = Sets.difference(queryMap.keySet(), keyedServices.keySet());
        if (!unknownKeys.isEmpty()) {
            throw new SafeIllegalStateException("A batch of timestamps {} produced some transaction service keys which"
                    + " are unknown: {}. Known transaction service keys were {}.",
                    SafeArg.of("timestamps", startTimestamps),
                    SafeArg.of("unknownKeys", unknownKeys),
                    SafeArg.of("knownServiceKeys", keyedServices.keySet()));
        }

        List<ListenableFuture<Map<Long, Long>>> futures = KeyedStream.stream(queryMap.asMap())
                .entries()
                .map(entry -> cellLoader.get(keyedServices.get(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());

        return Futures.whenAllSucceed(futures).call(
                () -> futures.stream()
                        .map(AtlasFutures::getDone)
                        .collect(HashMap::new, Map::putAll, Map::putAll),
                MoreExecutors.directExecutor());
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        TransactionService service = getServiceForTimestamp(startTimestamp).orElseThrow(
                () -> new UnsupportedOperationException("putUnlessExists shouldn't be used with null services"));
        service.putUnlessExists(startTimestamp, commitTimestamp);
    }

    @Override
    public void close() {
        keyedServices.values().forEach(TransactionService::close);
    }

    private Optional<TransactionService> getServiceForTimestamp(long startTimestamp) {
        T key = timestampToServiceKey.apply(startTimestamp);
        if (key == null) {
            return Optional.empty();
        }
        TransactionService service = keyedServices.get(key);

        if (service == null) {
            throw new SafeIllegalStateException("Could not find a transaction service for timestamp {}, which"
                    + " produced a key of {}. Known transaction service keys were {}.",
                    SafeArg.of("timestamp", startTimestamp),
                    SafeArg.of("serviceKey", key),
                    SafeArg.of("knownServiceKeys", keyedServices.keySet()));
        }
        return Optional.of(service);
    }

    private interface TimestampLoader {

        ListenableFuture<Long> get(TransactionService transactionService, long startTimestamp);

        ListenableFuture<Map<Long, Long>> get(TransactionService transactionService, Iterable<Long> startTimestamps);
    }
}
