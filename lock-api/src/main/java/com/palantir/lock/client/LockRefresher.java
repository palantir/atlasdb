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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockRefresher implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(LockRefresher.class);

    private final ScheduledExecutorService executor;
    private final TimelockService timelockService;
    private final Set<LockToken> tokensToRefresh = ConcurrentHashMap.newKeySet();

    public LockRefresher(
            ScheduledExecutorService executor,
            TimelockService timelockService,
            long refreshIntervalMillis) {
        this.executor = executor;
        this.timelockService = timelockService;

        scheduleRefresh(refreshIntervalMillis);
    }

    private void scheduleRefresh(long refreshIntervalMillis) {
        executor.scheduleAtFixedRate(
                this::refreshLocks,
                refreshIntervalMillis,
                refreshIntervalMillis,
                TimeUnit.MILLISECONDS);
    }

    private void refreshLocks() {
        try {
            Set<LockToken> toRefresh = ImmutableSet.copyOf(tokensToRefresh);
            if (toRefresh.isEmpty()) {
                return;
            }

            Set<LockToken> successfullyRefreshedTokens = timelockService.refreshLockLeases(toRefresh);
            Set<LockToken> refreshFailures = Sets.difference(toRefresh, successfullyRefreshedTokens);
            tokensToRefresh.removeAll(refreshFailures);
            if (!refreshFailures.isEmpty()) {
                log.info("Successfully refreshed {}, but failed to refresh {} lock tokens, "
                                + "most likely because they were lost on the server."
                                + " The first (up to) 20 of these were {}.",
                        SafeArg.of("successfullyRefreshed", successfullyRefreshedTokens.size()),
                        SafeArg.of("numLockTokens", refreshFailures.size()),
                        SafeArg.of("firstFailures",
                                Iterables.transform(Iterables.limit(refreshFailures, 20), LockToken::getRequestId)));
            }
        } catch (Throwable error) {
            log.warn("Error while refreshing locks. Trying again on next iteration", error);
        }
    }

    public void registerLocks(Collection<LockToken> tokens) {
        tokensToRefresh.addAll(tokens);
    }

    public void unregisterLocks(Collection<LockToken> tokens) {
        tokensToRefresh.removeAll(tokens);
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
