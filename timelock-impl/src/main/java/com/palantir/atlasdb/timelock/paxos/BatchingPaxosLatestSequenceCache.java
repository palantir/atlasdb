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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosLong;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
final class BatchingPaxosLatestSequenceCache implements CoalescingRequestFunction<Client, PaxosLong> {

    private static final SafeLogger log = SafeLoggerFactory.get(BatchingPaxosLatestSequenceCache.class);
    private static final PaxosLong DEFAULT_VALUE = PaxosLong.of(BatchPaxosAcceptor.NO_LOG_ENTRY);

    private final BatchPaxosAcceptor delegate;

    // we accumulate all clients that we've seen so far such that if we need to invalidate the cache and request a new
    // update, we receive a cache update that's consistent with the clients we've seen e.g. in the face of remote node
    // restarts or cache expirations
    private final Set<Client> clientsSeenSoFar = ConcurrentHashMap.newKeySet();

    // represents the cache digest with the highest timestamp that this client side cache has processed
    private final AtomicReference<TimestampedAcceptorCacheKey> latestCacheKey = new AtomicReference<>();

    // each value refers to a materialised version of the cache in terms of namespaces and the latest sequence at that
    // particular cache timestamp
    // as an optimisation to avoid copying, we can share the same materialised view for a string of cache keys issued
    // by the same instance of the server (i.e. not restarted, cache keys haven't expired).
    private final Cache<AcceptorCacheKey, ConcurrentMap<Client, PaxosLong>> cacheKeysToCaches =
            Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(1)).build();

    BatchingPaxosLatestSequenceCache(BatchPaxosAcceptor delegate) {
        this.delegate = delegate;
    }

    @Override
    public Map<Client, PaxosLong> apply(Set<Client> requestedClients) {
        clientsSeenSoFar.addAll(requestedClients);

        int attempt = 0;
        // in practice this loop should only ever run at most twice, if we need more, we can try a last ditch effort,
        // then subsequently fail the call which will be tried again
        while (attempt < 3) {
            TimestampedAcceptorCacheKey timestampedCacheKey = latestCacheKey.get();
            try {
                if (timestampedCacheKey == null) {
                    return populateNewCache(requestedClients);
                } else {
                    ConcurrentMap<Client, PaxosLong> maybeCache =
                            cacheKeysToCaches.getIfPresent(timestampedCacheKey.cacheKey());
                    // if there is no cache entry, then it's been evicted => we need to start again, otherwise we'll
                    // create a new map
                    if (maybeCache == null) {
                        latestCacheKey.compareAndSet(timestampedCacheKey, null);
                        return populateNewCache(requestedClients);
                    } else {
                        return populateExistingCache(timestampedCacheKey, maybeCache, requestedClients);
                    }
                }
            } catch (InvalidAcceptorCacheKeyException e) {
                log.info("Cache key is invalid, invalidating cache and retrying", SafeArg.of("attempt", attempt), e);
                // another request might have already reset or updated the cache key if this call is slow,
                // let's not waste their work
                latestCacheKey.compareAndSet(timestampedCacheKey, null);
                attempt++;
            }
        }

        throw new SafeIllegalStateException("could not complete request due to contention in the cache");
    }

    private Map<Client, PaxosLong> populateNewCache(Set<Client> requestedClients)
            throws InvalidAcceptorCacheKeyException {
        AcceptorCacheDigest digest = delegate.latestSequencesPreparedOrAccepted(Optional.empty(), clientsSeenSoFar);

        // use a new cache for the new digest with no previous state, if there are multiple in-flight requests at this
        // juncture, they will share this new cache as it's the same cache key
        // the *only* place in which we should create a map entry with a *new* map is when we're populating a new cache
        // everything else should be built on the previous entries or fail and reach this part.
        ConcurrentMap<Client, PaxosLong> newEntriesToCache =
                cacheKeysToCaches.get(digest.newCacheKey(), $ -> new ConcurrentHashMap<>());
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
            processDigest(
                    currentCachedEntries,
                    delegate.latestSequencesPreparedOrAccepted(
                            Optional.of(timestampedCacheKey.cacheKey()), newClients));
            return getResponseMap(currentCachedEntries, requestedClients);
        }
    }

    private void processDigest(ConcurrentMap<Client, PaxosLong> currentCachedEntries, AcceptorCacheDigest digest) {
        TimestampedAcceptorCacheKey newCacheKey = TimestampedAcceptorCacheKey.of(digest);
        // this shares the same map with "previous" cache keys under the current uptime of the remote
        ConcurrentMap<Client, PaxosLong> newCachedEntries =
                cacheKeysToCaches.get(newCacheKey.cacheKey(), $ -> currentCachedEntries);

        // merge local version of the cache with whatever updates even if they are "old" in terms of sequence numbers
        // this ensures that for each client the paxos sequence numbers are always increasing
        KeyedStream.stream(digest.updates())
                .map(PaxosLong::of)
                .forEach((client, paxosLong) ->
                        newCachedEntries.merge(client, paxosLong, BatchingPaxosLatestSequenceCache::max));

        // for a *new* mapping, setting the cache key must happen *after* we've setup the mapping, so that concurrent
        // clients will not reference an in-progress populating map which can be empty. If anything they can make a
        // request with the previous cacheKey, and do a bit of wasted work whilst we update the cache key.
        accumulateLatestCacheKey(newCacheKey);
    }

    private void accumulateLatestCacheKey(TimestampedAcceptorCacheKey newCacheKey) {
        while (true) {
            TimestampedAcceptorCacheKey current = latestCacheKey.get();
            // either the new cache key is older or the same as the current
            // or we race to set it and try again if we lose
            if ((current != null && newCacheKey.timestamp() <= current.timestamp())
                    || latestCacheKey.compareAndSet(current, newCacheKey)) {
                return;
            }
        }
    }

    private static Map<Client, PaxosLong> getResponseMap(
            ConcurrentMap<Client, PaxosLong> currentCachedEntries, Set<Client> requestedClients) {
        return Maps.toMap(requestedClients, client -> currentCachedEntries.getOrDefault(client, DEFAULT_VALUE));
    }

    private static PaxosLong max(PaxosLong a, PaxosLong b) {
        return Stream.of(a, b)
                .max(Comparator.comparingLong(PaxosLong::getValue))
                .orElseThrow(() -> new SafeIllegalArgumentException("No Paxos Value could be picked"));
    }
}
