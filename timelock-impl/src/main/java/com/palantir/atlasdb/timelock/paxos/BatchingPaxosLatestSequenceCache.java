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

package com.palantir.atlasdb.timelock.paxos;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.errors.RemoteException;
import com.palantir.paxos.PaxosLong;

@NotThreadSafe
final class BatchingPaxosLatestSequenceCache implements CoalescingRequestFunction<Client, PaxosLong> {

    @VisibleForTesting
    static final String ERROR_NAME = "TimelockPartitioning:InvalidCacheKey";

    private static final Logger log = LoggerFactory.getLogger(BatchingPaxosLatestSequenceCache.class);

    @Nullable
    private AcceptorCacheKey cacheKey = null;
    private Map<Client, PaxosLong> cachedEntries = Maps.newHashMap();
    private BatchPaxosAcceptor delegate;

    BatchingPaxosLatestSequenceCache(BatchPaxosAcceptor delegate) {
        this.delegate = delegate;
    }

    @Override
    public PaxosLong defaultValue() {
        return PaxosLong.of(BatchPaxosAcceptor.NO_LOG_ENTRY);
    }

    @Override
    public Map<Client, PaxosLong> apply(Set<Client> clients) {
        try {
            return unsafeGetLatest(clients);
        } catch (RemoteException e) {
            if (e.getError().errorName().equals(ERROR_NAME)) {
                log.info("Cache key is invalid, invalidating cache");
                cacheKey = null;
                Set<Client> allClients = ImmutableSet.<Client>builder()
                        .addAll(clients)
                        .addAll(cachedEntries.keySet())
                        .build();
                cachedEntries.clear();
                return unsafeGetLatest(allClients);
            }

            throw e;
        }
    }

    private Map<Client, PaxosLong> unsafeGetLatest(Set<Client> clients) {
        if (cacheKey == null) {
            processDigest(delegate.latestSequencesPreparedOrAccepted(Optional.empty(), clients));
            return Collections.unmodifiableMap(cachedEntries); // do we want a copy here? :S
        }

        Set<Client> newClients = Sets.difference(clients, cachedEntries.keySet());
        if (newClients.isEmpty()) {
            delegate.latestSequencesPreparedOrAcceptedCached(cacheKey).ifPresent(this::processDigest);
            return Collections.unmodifiableMap(cachedEntries);
        } else {
            processDigest(delegate.latestSequencesPreparedOrAccepted(Optional.of(cacheKey), newClients));
            return Collections.unmodifiableMap(cachedEntries);
        }
    }

    private void processDigest(AcceptorCacheDigest digest) {
        cacheKey = AcceptorCacheKey.of(digest.newCacheKey());
        Map<Client, PaxosLong> asPaxosLong = KeyedStream.stream(digest.updates())
                .map(PaxosLong::of)
                .collectToMap();
        cachedEntries.putAll(asPaxosLong);
    }
}
