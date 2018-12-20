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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Streams;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.AutoDelegate_TimelockService;
import com.palantir.lock.v2.ImmutableLockRequest;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;

public final class PartitioningTimelockService implements AutoDelegate_TimelockService {
    private static final int LOCK_BATCH_THRESHOLD = 1000;
    private static final int UNLOCK_BATCH_THRESHOLD = 10_000;

    private final Clock clock;
    private final TimelockService timelockService;

    // tokens used as identifiers; these don't necessarily directly exist in timelock
    private final ConcurrentMap<LockToken, Set<LockToken>> proxyLockTokens = new ConcurrentHashMap<>();

    @VisibleForTesting
    PartitioningTimelockService(Clock clock, TimelockService timelockService) {
        this.clock = clock;
        this.timelockService = timelockService;
    }

    public PartitioningTimelockService(TimelockService timelockService) {
        this(Clock.systemUTC(), timelockService);
    }

    @Override
    public TimelockService delegate() {
        return timelockService;
    }

    private Set<LockToken> addProxies(Set<LockToken> maybeProxies) {
        if (proxyLockTokens.isEmpty()) {
            return maybeProxies;
        } else {
            return maybeProxies.stream()
                    .flatMap(token ->
                            Streams.concat(
                                    Stream.of(token),
                                    Optional.ofNullable(proxyLockTokens.remove(token))
                                            .orElse(Collections.emptySet()).stream()))
                    .collect(toSet());
        }
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> unenrichedTokens) {
        Set<LockToken> tokens = addProxies(unenrichedTokens);
        if (tokens.size() <= UNLOCK_BATCH_THRESHOLD) {
            return timelockService.unlock(tokens);
        }
        return Streams.stream(Iterables.partition(tokens, UNLOCK_BATCH_THRESHOLD))
                .map(ImmutableSet::copyOf)
                .map(timelockService::unlock)
                .flatMap(Collection::stream)
                .collect(toSet());
    }

    @Override
    public void tryUnlock(Set<LockToken> unenrichedTokens) {
        Set<LockToken> tokens = addProxies(unenrichedTokens);
        Streams.stream(Iterables.partition(tokens, UNLOCK_BATCH_THRESHOLD))
                .map(ImmutableSet::copyOf)
                .forEach(timelockService::tryUnlock);
    }

    @Override
    public LockResponse lock(LockRequest request) {
        if (request.getLockDescriptors().size() <= LOCK_BATCH_THRESHOLD) {
            return timelockService.lock(request);
        }

        Instant startTime = clock.instant();
        Duration initialAcquireTimeout = Duration.ofMillis(request.getAcquireTimeoutMs());
        List<LockDescriptor> sortedLockDescriptors =
                Ordering.natural().immutableSortedCopy(request.getLockDescriptors());
        BatchedLockDescriptors descriptors = new BatchedLockDescriptors();
        try {
            for (List<LockDescriptor> batch : Iterables.partition(sortedLockDescriptors, LOCK_BATCH_THRESHOLD)) {
                Optional<Duration> acquireTimeout = acquireTimeout(startTime, initialAcquireTimeout);
                if (!acquireTimeout.isPresent()) {
                    return descriptors.failure();
                }

                LockRequest batchedRequest = ImmutableLockRequest.builder()
                        .from(request)
                        .acquireTimeoutMs(acquireTimeout.get().toMillis())
                        .lockDescriptors(batch)
                        .build();
                LockResponse response = timelockService.lock(batchedRequest);
                if (!response.wasSuccessful()) {
                    return descriptors.failure();
                }
                descriptors.add(response.getToken());
            }
        } catch (RuntimeException e) {
            throw descriptors.failure(e);
        }
        return descriptors.success();
    }

    private final class BatchedLockDescriptors {
        private final Set<LockToken> tokensFetched = new HashSet<>();

        void add(LockToken token) {
            tokensFetched.add(token);
        }

        LockResponse success() {
            LockToken proxyToken = LockToken.of(UUID.randomUUID());
            proxyLockTokens.put(proxyToken, tokensFetched);
            return LockResponse.successful(proxyToken);
        }

        LockResponse failure() {
            tryUnlock(tokensFetched);
            return LockResponse.timedOut();
        }

        RuntimeException failure(RuntimeException e) {
            tryUnlock(tokensFetched);
            throw e;
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
