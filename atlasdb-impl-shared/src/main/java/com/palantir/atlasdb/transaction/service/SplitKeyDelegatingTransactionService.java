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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.CheckForNull;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

/**
 * A {@link SplitKeyDelegatingTransactionService} delegates between multiple {@link TransactionService}s, depending
 * on which timestamps are requested. This class preserves the {@link TransactionService} guarantees regardless of
 * which underlying service is contacted.
 *
 * The timestamp service will throw an exception if the timestamp-to-service-key function returns a key which is
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
        return getServiceForTimestamp(startTimestamp).get(startTimestamp);
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        // TODO (jkong): If this is too slow, don't use streams.
        Map<T, List<Long>> queryMap = StreamSupport.stream(startTimestamps.spliterator(), false)
                .collect(Collectors.groupingBy(timestampToServiceKey));

        Set<T> unknownKeys = Sets.difference(queryMap.keySet(), keyedServices.keySet());
        if (!unknownKeys.isEmpty()) {
            throw new SafeIllegalStateException("A batch of timestamps {} produced some transaction service keys which"
                    + " are unknown: {}. Known transaction service keys were {}.",
                    SafeArg.of("timestamps", startTimestamps),
                    SafeArg.of("unknownKeys", unknownKeys),
                    SafeArg.of("knownServiceKeys", keyedServices.keySet()));
        }

        return queryMap.entrySet()
                .stream()
                .map(entry -> keyedServices.get(entry.getKey()).get(entry.getValue()))
                .collect(HashMap::new, Map::putAll, Map::putAll);
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        getServiceForTimestamp(startTimestamp).putUnlessExists(startTimestamp, commitTimestamp);
    }

    private TransactionService getServiceForTimestamp(long startTimestamp) {
        T key = timestampToServiceKey.apply(startTimestamp);
        TransactionService service = keyedServices.get(key);

        if (service == null) {
            throw new SafeIllegalStateException("Could not find a transaction service for timestamp {}, which"
                    + " produced a key of {}. Known transaction service keys were {}.",
                    SafeArg.of("timestamp", startTimestamp),
                    SafeArg.of("serviceKey", key),
                    SafeArg.of("knownServiceKeys", keyedServices.keySet()));
        }
        return service;
    }
}
