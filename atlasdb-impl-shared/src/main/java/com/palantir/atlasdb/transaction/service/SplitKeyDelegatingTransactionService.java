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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.CheckForNull;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
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

    public SplitKeyDelegatingTransactionService(
            Function<Long, T> timestampToServiceKey,
            Map<T, TransactionService> keyedServices) {
        this.timestampToServiceKey = timestampToServiceKey;
        this.keyedServices = keyedServices;
    }

    @CheckForNull
    @Override
    public Long get(long startTimestamp) {
        return getServiceForTimestamp(startTimestamp).map(service -> service.get(startTimestamp)).orElse(null);
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
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

        return queryMap.asMap()
                .entrySet()
                .stream()
                .map(entry -> keyedServices.get(entry.getKey()).get(entry.getValue()))
                .collect(HashMap::new, Map::putAll, Map::putAll);
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
}
