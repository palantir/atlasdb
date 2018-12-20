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

package com.palantir.lock.client;

import static java.util.stream.Collectors.toSet;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.AutoDelegate_TimelockService;
import com.palantir.lock.v2.ImmutableLockRequest;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;

public final class PartitioningTimelockService implements AutoDelegate_TimelockService {
    private static final int LOCK_BATCH_THRESHOLD = 5000;
    private static final int UNLOCK_BATCH_THRESHOLD = 10_000;

    private final Clock clock;
    private final TimelockService timelockService;
    private final Supplier<LockToken> proxyLockTokenSupplier;
    private final Supplier<UUID> requestIdSupplier;

    private final int lockBatchThreshold;
    private final int unlockBatchThreshold;

    // tokens used as identifiers; these don't necessarily directly exist in timelock
    private final ProxyLockTokens proxyLockTokens = new ProxyLockTokens();

    private static final class ProxyLockTokens {
        private final ConcurrentMap<LockToken, Set<LockToken>> tokens = new ConcurrentHashMap<>();

        void put(LockToken proxyToken, Set<LockToken> realTokens) {
            tokens.put(proxyToken, realTokens);
        }

        /**
         * Returns a map of token (possibly a proxy) to the user facing token we handed out for it.
         */
        Map<LockToken, LockToken> replaceProxies(Set<LockToken> maybeProxies) {
            Map<LockToken, LockToken> results = new LinkedHashMap<>();
            for (LockToken token : maybeProxies) {
                Set<LockToken> maybeProxy = tokens.remove(token);
                if (maybeProxy == null) {
                    results.put(token, token);
                } else {
                    results.putAll(Maps.asMap(maybeProxy, unused -> token));
                }
            }
            return results;
        }
    }

    @VisibleForTesting
    PartitioningTimelockService(
            Clock clock,
            TimelockService timelockService,
            Supplier<LockToken> proxyLockTokenSupplier,
            Supplier<UUID> requestIdSupplier,
            int lockBatchThreshold,
            int unlockBatchThresholed) {
        this.clock = clock;
        this.timelockService = timelockService;
        this.proxyLockTokenSupplier = proxyLockTokenSupplier;
        this.requestIdSupplier = requestIdSupplier;
        this.lockBatchThreshold = lockBatchThreshold;
        this.unlockBatchThreshold = unlockBatchThresholed;
    }

    public PartitioningTimelockService(TimelockService timelockService) {
        this(Clock.systemUTC(),
                timelockService,
                () -> LockToken.of(UUID.randomUUID()),
                UUID::randomUUID,
                LOCK_BATCH_THRESHOLD,
                UNLOCK_BATCH_THRESHOLD);
    }

    @Override
    public TimelockService delegate() {
        return timelockService;
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> unenrichedTokens) {
        Map<LockToken, LockToken> tokens = proxyLockTokens.replaceProxies(unenrichedTokens);
        if (tokens.size() <= unlockBatchThreshold) {
            return timelockService.unlock(tokens.keySet());
        }
        Set<LockToken> unsuccessfulTokens = Streams.stream(Iterables.partition(tokens.keySet(), unlockBatchThreshold))
                .map(ImmutableSet::copyOf)
                .map(t -> Sets.difference(t, timelockService.unlock(t)))
                .flatMap(Collection::stream)
                .map(tokens::get)
                .collect(toSet());
        return Sets.difference(ImmutableSet.copyOf(tokens.values()), unsuccessfulTokens);
    }

    @Override
    public void tryUnlock(Set<LockToken> unenrichedTokens) {
        Set<LockToken> tokens = proxyLockTokens.replaceProxies(unenrichedTokens).keySet();
        Streams.stream(Iterables.partition(tokens, unlockBatchThreshold))
                .map(ImmutableSet::copyOf)
                .forEach(timelockService::tryUnlock);
    }

    @Override
    public LockResponse lock(LockRequest request) {
        if (request.getLockDescriptors().size() <= lockBatchThreshold) {
            return timelockService.lock(request);
        }

        Instant startTime = clock.instant();
        Duration initialAcquireTimeout = Duration.ofMillis(request.getAcquireTimeoutMs());
        List<LockDescriptor> sortedLockDescriptors =
                Ordering.natural().immutableSortedCopy(request.getLockDescriptors());
        BatchedLockDescriptors descriptors = new BatchedLockDescriptors();
        try {
            for (List<LockDescriptor> batch : Iterables.partition(sortedLockDescriptors, lockBatchThreshold)) {
                Optional<Duration> acquireTimeout = acquireTimeout(startTime, initialAcquireTimeout);
                if (!acquireTimeout.isPresent()) {
                    return descriptors.failure(LockResponse.timedOut());
                }

                LockRequest batchedRequest = ImmutableLockRequest.builder()
                        .requestId(requestIdSupplier.get())
                        .acquireTimeoutMs(acquireTimeout.get().toMillis())
                        .lockDescriptors(batch)
                        .build();
                LockResponse response = timelockService.lock(batchedRequest);
                if (!response.wasSuccessful()) {
                    return descriptors.failure(response);
                }
                descriptors.add(response.getToken());
            }
        } catch (RuntimeException e) {
            throw descriptors.failure(e);
        }
        return descriptors.success();
    }

    private final class BatchedLockDescriptors {
        private final Set<LockToken> tokensFetched = new LinkedHashSet<>();

        void add(LockToken token) {
            tokensFetched.add(token);
        }

        LockResponse success() {
            LockToken proxyToken = proxyLockTokenSupplier.get();
            proxyLockTokens.put(proxyToken, tokensFetched);
            return LockResponse.successful(proxyToken);
        }

        LockResponse failure(LockResponse response) {
            timelockService.tryUnlock(tokensFetched);
            return response;
        }

        RuntimeException failure(RuntimeException exception) {
            timelockService.tryUnlock(tokensFetched);
            throw exception;
        }
    }

    private Optional<Duration> acquireTimeout(Instant startTime, Duration initialAcquireTimeout) {
        if (initialAcquireTimeout.isZero()) {
            return Optional.of(Duration.ZERO);
        }
        Instant deadline = startTime.plus(initialAcquireTimeout);
        return Optional.of(clock.instant())
                .map(instant -> Duration.between(instant, deadline))
                .filter(d -> d.compareTo(Duration.ZERO) > 0);
    }
}
