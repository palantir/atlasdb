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
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;
import com.palantir.common.streams.KeyedStream;

public class AcceptorCacheImpl implements AcceptorCache {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Cache<AcceptorCacheKey, Long> cacheKeyToTimestamp;
    private final Map<Client, WithSeq<Long>> clientToTimeAndSeq = Maps.newHashMap();
    private final TreeMultimap<Long, WithSeq<Client>> clientsByLatestTimestamp = TreeMultimap.create(
            Comparator.naturalOrder(),
            Comparator.comparing(clientWithSeq -> clientWithSeq.value().value(), Comparator.naturalOrder()));

    @GuardedBy("lock")
    private long currentTimestamp = 0;
    @GuardedBy("lock")
    private AcceptorCacheKey latestAcceptorCacheKey = AcceptorCacheKey.newCacheKey();

    public AcceptorCacheImpl() {
        Cache<AcceptorCacheKey, Long> cacheKeyToTime = Caffeine.newBuilder()
                .expireAfterAccess(Duration.ofMinutes(10))
                .build();
        cacheKeyToTime.put(latestAcceptorCacheKey, currentTimestamp);
        this.cacheKeyToTimestamp = cacheKeyToTime;
    }

    @Override
    public void updateSequenceNumbers(Set<WithSeq<Client>> clientsAndSeqs) {
        lock.writeLock().lock();
        try {
            long nextTimestamp = currentTimestamp + 1;
            AtomicBoolean updated = new AtomicBoolean(false);

            clientsAndSeqs.forEach(clientAndSeq -> {
                Client client = clientAndSeq.value();
                long incomingSequenceNumber = clientAndSeq.seq();
                WithSeq<Long> clientLatestWithTs = clientToTimeAndSeq.get(client);

                if (clientLatestWithTs == null) {
                    clientToTimeAndSeq.put(client, WithSeq.of(nextTimestamp, incomingSequenceNumber));
                    clientsByLatestTimestamp.put(nextTimestamp, clientAndSeq);
                    updated.set(true);
                } else if (incomingSequenceNumber > clientLatestWithTs.seq()) {
                    clientToTimeAndSeq.put(client, WithSeq.of(nextTimestamp, incomingSequenceNumber));
                    clientsByLatestTimestamp.put(nextTimestamp, clientAndSeq);
                    clientsByLatestTimestamp.remove(
                            clientLatestWithTs.value(),
                            WithSeq.of(client, clientLatestWithTs.seq()));
                    updated.set(true);
                }
            });

            if (!updated.get()) {
                return;
            }

            AcceptorCacheKey nextCacheKey = AcceptorCacheKey.newCacheKey();
            cacheKeyToTimestamp.put(nextCacheKey, nextTimestamp);

            currentTimestamp = nextTimestamp;
            latestAcceptorCacheKey = nextCacheKey;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public AcceptorCacheDigest getAllUpdates() {
        lock.readLock().lock();
        try {
            Map<Client, Long> clientsToLatest = KeyedStream.stream(clientToTimeAndSeq)
                    .map(WithSeq::seq)
                    .collectToMap();
            return ImmutableAcceptorCacheDigest.builder()
                    .newCacheKey(latestAcceptorCacheKey)
                    .updates(clientsToLatest)
                    .build();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<AcceptorCacheDigest> updatesSinceCacheKey(AcceptorCacheKey cacheKey)
            throws InvalidAcceptorCacheKeyException {
        lock.readLock().lock();
        try {
            if (cacheKey.equals(latestAcceptorCacheKey)) {
                return Optional.empty();
            }

            Long cacheKeyTimestamp = cacheKeyToTimestamp.getIfPresent(cacheKey);
            if (cacheKeyTimestamp == null) {
                throw new InvalidAcceptorCacheKeyException(cacheKey);
            }

            Map<Client, Long> diff = clientsByLatestTimestamp.asMap().tailMap(cacheKeyTimestamp, false).values()
                    .stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toMap(WithSeq::value, WithSeq::seq));

            return Optional.of(ImmutableAcceptorCacheDigest.builder()
                    .newCacheKey(latestAcceptorCacheKey)
                    .updates(diff)
                    .build());
        } finally {
            lock.readLock().unlock();
        }
    }

}
