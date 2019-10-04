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

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.PaxosLong;

/*
    This is not thread safe, but it is okay because it is run within an autobatcher, which is configured to not process
    multiple batches in parallel.
 */
@NotThreadSafe
final class BatchingPaxosLatestSequenceCache implements CoalescingRequestFunction<Client, PaxosLong> {

    private static final Logger log = LoggerFactory.getLogger(BatchingPaxosLatestSequenceCache.class);
    private static final PaxosLong DEFAULT_VALUE = PaxosLong.of(BatchPaxosAcceptor.NO_LOG_ENTRY);

    @Nullable
    private AcceptorCacheKey cacheKey = null;
    private Map<Client, PaxosLong> cachedEntries = Maps.newHashMap();
    private BatchPaxosAcceptor delegate;

    BatchingPaxosLatestSequenceCache(BatchPaxosAcceptor delegate) {
        this.delegate = delegate;
    }

    @Override
    public Map<Client, PaxosLong> apply(Set<Client> clients) {
        try {
            return unsafeGetLatest(clients);
        } catch (InvalidAcceptorCacheKeyException e) {
            log.info("Cache key is invalid, invalidating cache - using deprecated detection method");
            return handleCacheMiss(clients);
        }
    }

    private Map<Client, PaxosLong> handleCacheMiss(Set<Client> requestedClients) {
        cacheKey = null;
        Set<Client> allClients = ImmutableSet.<Client>builder()
                .addAll(requestedClients)
                .addAll(cachedEntries.keySet())
                .build();
        cachedEntries.clear();
        try {
            return unsafeGetLatest(allClients);
        } catch (InvalidAcceptorCacheKeyException e) {
            log.warn("Empty cache key is still invalid indicates product bug, failing request.");
            throw new RuntimeException(e);
        }
    }

    private Map<Client, PaxosLong> unsafeGetLatest(Set<Client> clients) throws InvalidAcceptorCacheKeyException {
        if (cacheKey == null) {
            processDigest(delegate.latestSequencesPreparedOrAccepted(Optional.empty(), clients));
            return getResponseMap(clients);
        }

        Set<Client> newClients = Sets.difference(clients, cachedEntries.keySet());
        if (newClients.isEmpty()) {
            delegate.latestSequencesPreparedOrAcceptedCached(cacheKey).ifPresent(this::processDigest);
            return getResponseMap(clients);
        } else {
            processDigest(delegate.latestSequencesPreparedOrAccepted(Optional.of(cacheKey), newClients));
            return getResponseMap(clients);
        }
    }

    private Map<Client, PaxosLong> getResponseMap(Set<Client> clientsInRequest) {
        return Maps.toMap(clientsInRequest, client -> cachedEntries.getOrDefault(client, DEFAULT_VALUE));
    }

    private void processDigest(AcceptorCacheDigest digest) {
        cacheKey = digest.newCacheKey();
        Map<Client, PaxosLong> asPaxosLong = KeyedStream.stream(digest.updates())
                .map(PaxosLong::of)
                .collectToMap();
        cachedEntries.putAll(asPaxosLong);
    }
}
