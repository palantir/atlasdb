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

import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.paxos.PaxosLong;

@ThreadSafe
final class BatchingPaxosLatestSequenceCache implements CoalescingRequestFunction<Client, PaxosLong> {

    private static final Logger log = LoggerFactory.getLogger(BatchingPaxosLatestSequenceCache.class);
    private static final PaxosLong DEFAULT_VALUE = PaxosLong.of(BatchPaxosAcceptor.NO_LOG_ENTRY);

    private final BatchPaxosAcceptor delegate;

    private final Set<Client> clientsSeenSoFar = Sets.newConcurrentHashSet();

    private final AtomicReference<TimestampedAcceptorCacheKey> cacheKey = new AtomicReference<>();
    private final LoadingCache<TimestampedAcceptorCacheKey, ConcurrentMap<Client, PaxosLong>> cacheKeysToCaches =
            Caffeine.newBuilder()
                    .expireAfterAccess(Duration.ofMinutes(1))
                    .build($ -> Maps.newConcurrentMap());

    BatchingPaxosLatestSequenceCache(BatchPaxosAcceptor delegate) {
        this.delegate = delegate;
    }

    @Override
    public Map<Client, PaxosLong> apply(Set<Client> requestedClients) {
        // always add requested clients so we can easily query with everything we've ever seen when our cache is invalid
        clientsSeenSoFar.addAll(requestedClients);

        int attempt = 0;
        while (attempt < 3) {
            TimestampedAcceptorCacheKey timestampedCacheKey = cacheKey.get();
            try {
                if (timestampedCacheKey == null) {
                    return populateNewCache(requestedClients);
                } else {
                    return populateExistingCache(
                            timestampedCacheKey,
                            cacheKeysToCaches.get(timestampedCacheKey),
                            requestedClients);
                }
            } catch (InvalidAcceptorCacheKeyException e) {
                log.info("Cache key is invalid, invalidating cache and retrying",
                        SafeArg.of("attempt", attempt),
                        e);
                cacheKey.compareAndSet(timestampedCacheKey, null);
                attempt++;
            }
        }

        throw new SafeIllegalStateException("could not request complete request due to contention in the cache");
    }

    private Map<Client, PaxosLong> populateNewCache(Set<Client> requestedClients)
            throws InvalidAcceptorCacheKeyException {
        AcceptorCacheDigest digest = delegate.latestSequencesPreparedOrAccepted(Optional.empty(), clientsSeenSoFar);
        ConcurrentMap<Client, PaxosLong> newEntriesToCache =
                cacheKeysToCaches.get(TimestampedAcceptorCacheKey.of(digest));
        processDigest(newEntriesToCache, digest);
        return getResponseMap(newEntriesToCache, requestedClients);
    }

    private Map<Client, PaxosLong> populateExistingCache(
            TimestampedAcceptorCacheKey timestampedCacheKey,
            ConcurrentMap<Client, PaxosLong> currentCachedEntries,
            Set<Client> requestedClients)
            throws InvalidAcceptorCacheKeyException {
        Set<Client> newClients = ImmutableSet.copyOf(Sets.difference(requestedClients, currentCachedEntries.keySet()));
        if (newClients.isEmpty()) {
            delegate.latestSequencesPreparedOrAcceptedCached(timestampedCacheKey.cacheKey())
                    .ifPresent(digest -> processDigest(currentCachedEntries, digest));
            return getResponseMap(currentCachedEntries, requestedClients);
        } else {
            processDigest(currentCachedEntries, delegate.latestSequencesPreparedOrAccepted(
                    Optional.of(timestampedCacheKey.cacheKey()),
                    newClients));
            return getResponseMap(currentCachedEntries, requestedClients);
        }
    }

    private void processDigest(ConcurrentMap<Client, PaxosLong> currentCachedEntries, AcceptorCacheDigest digest) {
        TimestampedAcceptorCacheKey newCacheKey = TimestampedAcceptorCacheKey.of(digest);
        // this shares the same map with "previous" cache keys, if it's too confusing we can always copy it potentially
        ConcurrentMap<Client, PaxosLong> newCachedEntries =
                cacheKeysToCaches.get(newCacheKey, $ -> currentCachedEntries);
        KeyedStream.stream(digest.updates())
                .map(PaxosLong::of)
                .forEach((client, paxosLong) ->
                        newCachedEntries.merge(client, paxosLong, BatchingPaxosLatestSequenceCache::max));

        // for a *new* mapping, setting the cache key must happen *after* we've setup the mapping, so that concurrent
        // clients will not reference an in-progress populating map which can be empty.
        maybeSetNewCacheKey(newCacheKey);
    }

    private void maybeSetNewCacheKey(TimestampedAcceptorCacheKey newCacheKey) {
        while (true) {
            TimestampedAcceptorCacheKey current = cacheKey.get();
            if ((current != null && newCacheKey.timestamp() <= current.timestamp())
                    || cacheKey.compareAndSet(current, newCacheKey)) {
                return;
            }
        }
    }

    private static Map<Client, PaxosLong> getResponseMap(
            ConcurrentMap<Client, PaxosLong> currentCachedEntries,
            Set<Client> requestedClients) {
        return Maps.toMap(requestedClients, client -> currentCachedEntries.getOrDefault(client, DEFAULT_VALUE));
    }

    private static PaxosLong max(PaxosLong a, PaxosLong b) {
        return Stream.of(a, b)
                .max(Comparator.comparingLong(PaxosLong::getValue))
                .orElseThrow(() -> { throw new SafeIllegalArgumentException("No Paxos Value could be picked"); });
    }

    @Value.Immutable
    interface TimestampedAcceptorCacheKey {
        @Value.Parameter
        AcceptorCacheKey cacheKey();

        // this exists to give ordering to cache keys especially when they're coming back concurrently.
        @Value.Parameter
        long timestamp();

        static TimestampedAcceptorCacheKey of(AcceptorCacheDigest digest) {
            return ImmutableTimestampedAcceptorCacheKey.of(digest.newCacheKey(), digest.cacheTimestamp());
        }
    }
}
