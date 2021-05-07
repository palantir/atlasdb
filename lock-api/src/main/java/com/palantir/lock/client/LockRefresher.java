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

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;
import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockRefresher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LockRefresher.class);

    private final ScheduledExecutorService executor;
    private final TimelockService timelockService;
    private final Map<LockToken, ClientLockingContext> tokensToClientContext = new ConcurrentHashMap<>();
    private final Clock clock;

    private ScheduledFuture<?> task;

    public LockRefresher(
            ScheduledExecutorService executor, TimelockService timelockService, long refreshIntervalMillis) {
        this(executor, timelockService, refreshIntervalMillis, Clock.systemUTC());
    }

    public LockRefresher(
            ScheduledExecutorService executor,
            TimelockService timelockService,
            long refreshIntervalMillis,
            Clock clock) {
        this.executor = executor;
        this.timelockService = timelockService;
        this.clock = clock;

        scheduleRefresh(refreshIntervalMillis);
    }

    private void scheduleRefresh(long refreshIntervalMillis) {
        task = executor.scheduleAtFixedRate(
                this::refreshLocks, refreshIntervalMillis, refreshIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private void refreshLocks() {
        try {
            Set<LockToken> toRefresh = getTokensToRefreshAndExpireStaleTokens();
            if (toRefresh.isEmpty()) {
                return;
            }

            Set<LockToken> successfullyRefreshedTokens = timelockService.refreshLockLeases(toRefresh);
            Set<LockToken> refreshFailures = Sets.difference(toRefresh, successfullyRefreshedTokens);
            refreshFailures.forEach(tokensToClientContext::remove);
            if (!refreshFailures.isEmpty()) {
                log.info(
                        "Successfully refreshed {}, but failed to refresh {} lock tokens, "
                                + "most likely because they were lost on the server."
                                + " The first (up to) 20 of these were {}.",
                        SafeArg.of("successfullyRefreshed", successfullyRefreshedTokens.size()),
                        SafeArg.of("numLockTokens", refreshFailures.size()),
                        SafeArg.of(
                                "firstFailures",
                                Iterables.transform(Iterables.limit(refreshFailures, 20), LockToken::getRequestId)));
            }
        } catch (Throwable error) {
            log.warn("Error while refreshing locks. Trying again on next iteration", error);
        }
    }

    // We could use a parallel tree-set, sorted on deadlines to get O(log n) performance for this operation while
    // preserving O(failure) time for removals. However, this is a background task that only runs once in a while,
    // and we need to serialise the entire structure when we refresh anyway, so I would not view the performance
    // differential as significant here.
    private Set<LockToken> getTokensToRefreshAndExpireStaleTokens() {
        Instant now = clock.instant();
        Set<LockToken> tokensToRefresh = new HashSet<>();
        for (Map.Entry<LockToken, ClientLockingContext> candidate : tokensToClientContext.entrySet()) {
            Instant deadline = candidate.getValue().lockRefreshDeadline();
            if (now.isAfter(deadline)) {
                log.info(
                        "A lock token has expired on the client, because it has exceeded its tenure: we will stop"
                                + " refreshing it automatically. Some time may still be required (20 seconds by"
                                + " default) before the server releases the associated lock grants.",
                        SafeArg.of("lockToken", candidate.getKey()),
                        SafeArg.of("expiryDeadline", deadline),
                        SafeArg.of("now", now));
                candidate.getValue().clientExpiryCallback().run();
            } else {
                tokensToRefresh.add(candidate.getKey());
            }
        }
        return tokensToRefresh;
    }

    public void registerLocks(Collection<LockToken> tokens) {
        registerLocks(tokens, ClientLockingOptions.getDefault());
    }

    public void registerLocks(Collection<LockToken> tokens, ClientLockingOptions lockingOptions) {
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

    public void unregisterLocks(Collection<LockToken> tokens) {
        tokens.forEach(tokensToClientContext::remove);
    }

    @Override
    public void close() {
        if (task != null) {
            task.cancel(false);
        }
    }

    @Value.Immutable
    interface ClientLockingContext {
        Instant lockRefreshDeadline();

        Runnable clientExpiryCallback();
    }
}
