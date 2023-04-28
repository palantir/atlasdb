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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.LockLeaseRefresher;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public class LockRefresher<T> implements AutoCloseable {

    private static final SafeLogger log = SafeLoggerFactory.get(LockRefresher.class);

    private final ScheduledExecutorService executor;
    private final LockLeaseRefresher<T> lockLeaseRefresher;
    private final Map<T, ClientLockingContext> tokensToClientContext = new ConcurrentHashMap<>();
    private final Clock clock;

    private ScheduledFuture<?> task;

    private BuggifyFactory buggify;

    public LockRefresher(
            ScheduledExecutorService executor,
            LockLeaseRefresher<T> lockLeaseRefresher,
            long refreshIntervalMillis,
            BuggifyFactory buggifyFactory) {
        this(executor, lockLeaseRefresher, refreshIntervalMillis, Clock.systemUTC(), buggifyFactory);
    }

    @VisibleForTesting
    LockRefresher(
            ScheduledExecutorService executor,
            LockLeaseRefresher<T> lockLeaseRefresher,
            long refreshIntervalMillis,
            Clock clock,
            BuggifyFactory buggifyFactory) {
        this.executor = executor;
        this.lockLeaseRefresher = lockLeaseRefresher;
        this.clock = clock;
        this.buggify = buggifyFactory;

        scheduleRefresh(refreshIntervalMillis);
    }

    private void scheduleRefresh(long refreshIntervalMillis) {
        task = executor.scheduleAtFixedRate(
                this::refreshLocks, refreshIntervalMillis, refreshIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private void refreshLocks() {
        try {
            Set<T> toRefresh = getTokensToRefreshAndExpireStaleTokens();
            if (toRefresh.isEmpty()) {
                return;
            }

            Set<T> successfullyRefreshedTokens = lockLeaseRefresher.refreshLockLeases(toRefresh);
            Set<T> refreshFailures = Sets.difference(toRefresh, successfullyRefreshedTokens);
            refreshFailures.forEach(tokensToClientContext::remove);
            if (!refreshFailures.isEmpty()) {
                log.info(
                        "Successfully refreshed {}, but failed to refresh {} lock tokens, "
                                + "most likely because they were lost on the server.",
                        SafeArg.of("successfullyRefreshed", successfullyRefreshedTokens.size()),
                        SafeArg.of("numLockTokens", refreshFailures.size()));
            }
            buggify.maybe(0.50).run(() -> unregisterLocks(toRefresh));
        } catch (Throwable error) {
            log.warn("Error while refreshing locks. Trying again on next iteration", error);
        }
    }

    // We could use a parallel tree-set, sorted on deadlines to get O(log n) performance for this operation while
    // preserving O(failure) time for removals. However, this is a background task that only runs once in a while,
    // and we need to serialise the entire structure when we refresh anyway, so I would not view the performance
    // differential as significant here.
    private Set<T> getTokensToRefreshAndExpireStaleTokens() {
        Instant now = clock.instant();
        Set<T> tokensToRefresh = new HashSet<>();
        for (Map.Entry<T, ClientLockingContext> candidate : tokensToClientContext.entrySet()) {
            Instant deadline = candidate.getValue().lockRefreshDeadline();
            if (now.isAfter(deadline)) {
                log.info(
                        "A lock token has expired on the client, because it has exceeded its tenure: we will stop"
                                + " refreshing it automatically. Some time may still be required (20 seconds by"
                                + " default) before the server releases the associated lock grants.",
                        getSafeArg(candidate.getKey()),
                        SafeArg.of("expiryDeadline", deadline),
                        SafeArg.of("now", now));
                candidate.getValue().clientExpiryCallback().run();
            } else {
                tokensToRefresh.add(candidate.getKey());
            }
        }

        Sets.difference(tokensToClientContext.keySet(), tokensToRefresh).forEach(tokensToClientContext::remove);
        return tokensToRefresh;
    }

    public void registerLocks(Collection<T> tokens) {
        registerLocks(tokens, ClientLockingOptions.getDefault());
    }

    public void registerLocks(Collection<T> tokens, ClientLockingOptions lockingOptions) {
        tokens = buggify.maybe(0.10)
                .map(() -> tokens.stream().filter(Random::nextBoolean).collect(Collectors.toCollection()));
        tokens.forEach(token -> tokensToClientContext.put(
                token,
                ImmutableClientLockingContext.builder()
                        .lockRefreshDeadline(lockingOptions
                                .maximumLockTenure()
                                .map(tenure -> clock.instant().plus(tenure))
                                .orElse(Instant.MAX))
                        .clientExpiryCallback(lockingOptions.tenureExpirationCallback())
                        .build()));
    }

    public void unregisterLocks(Collection<T> tokens) {
        tokens.forEach(tokensToClientContext::remove);
    }

    @Override
    public void close() {
        if (task != null) {
            task.cancel(false);
        }
    }

    private Arg<?> getSafeArg(T key) {
        if (key instanceof LockToken) {
            return ((LockToken) key).toSafeArg("lockToken");
        }

        return SafeArg.of("lockToken", key);
    }

    @Value.Immutable
    interface ClientLockingContext {
        Instant lockRefreshDeadline();

        Runnable clientExpiryCallback();
    }
}
