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
import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.Client;
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
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

public class AcceptorCacheImpl implements AcceptorCache {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Cache<AcceptorCacheKey, TimestampedAcceptorCacheKey> cacheKeyToTimestamp;
    private final Map<Client, WithSeq<Long>> clientToTimeAndSeq = Maps.newHashMap();
    private final TreeMultimap<Long, WithSeq<Client>> clientsByLatestTimestamp = TreeMultimap.create(
            Comparator.naturalOrder(),
            Comparator.comparing(clientWithSeq -> clientWithSeq.value().value(), Comparator.naturalOrder()));

    @GuardedBy("lock")
    private TimestampedAcceptorCacheKey latestTimestampedAcceptorCacheKey =
            TimestampedAcceptorCacheKey.of(AcceptorCacheKey.newCacheKey(), 0);

    public AcceptorCacheImpl() {
        Cache<AcceptorCacheKey, TimestampedAcceptorCacheKey> cacheKeyToTime = Caffeine.newBuilder()
                .expireAfterAccess(Duration.ofMinutes(10))
                .build();
        cacheKeyToTime.put(latestTimestampedAcceptorCacheKey.cacheKey(), latestTimestampedAcceptorCacheKey);
        this.cacheKeyToTimestamp = cacheKeyToTime;
    }

    @Override
    public void updateSequenceNumbers(Set<WithSeq<Client>> clientsAndSeqs) {
        if (clientsAndSeqs.isEmpty()) {
            return;
        }

        lock.writeLock().lock();
        try {
            long nextTimestamp = latestTimestampedAcceptorCacheKey.timestamp() + 1;
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
            TimestampedAcceptorCacheKey newTimestampedCacheKey =
                    TimestampedAcceptorCacheKey.of(nextCacheKey, nextTimestamp);
            cacheKeyToTimestamp.put(nextCacheKey, newTimestampedCacheKey);
            latestTimestampedAcceptorCacheKey = newTimestampedCacheKey;
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
                    .newCacheKey(latestTimestampedAcceptorCacheKey.cacheKey())
                    .cacheTimestamp(latestTimestampedAcceptorCacheKey.timestamp())
                    .updates(clientsToLatest)
                    .build();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<AcceptorCacheDigest> updatesSinceCacheKey(@Nonnull AcceptorCacheKey cacheKey)
            throws InvalidAcceptorCacheKeyException {
        lock.readLock().lock();
        try {
            if (cacheKey.equals(latestTimestampedAcceptorCacheKey.cacheKey())) {
                return Optional.empty();
            }

            long cacheKeyTimestamp = Optional.ofNullable(cacheKeyToTimestamp.getIfPresent(cacheKey))
                    .map(TimestampedAcceptorCacheKey::timestamp)
                    .orElseThrow(() -> new InvalidAcceptorCacheKeyException(cacheKey));

            Map<Client, Long> diff = clientsByLatestTimestamp.asMap().tailMap(cacheKeyTimestamp, false).values()
                    .stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toMap(WithSeq::value, WithSeq::seq));

            return Optional.of(ImmutableAcceptorCacheDigest.builder()
                    .newCacheKey(latestTimestampedAcceptorCacheKey.cacheKey())
                    .cacheTimestamp(latestTimestampedAcceptorCacheKey.timestamp())
                    .updates(diff)
                    .build());
        } finally {
            lock.readLock().unlock();
        }
    }

}
