/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.lock.client;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;

public class LockRefresher {

    private final Logger log = LoggerFactory.getLogger(LockRefresher.class);

    private final ScheduledExecutorService executor;
    private final TimelockService timelockService;
    private final Set<LockToken> tokensToRefresh = Sets.newConcurrentHashSet();

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

            Set<LockToken> refreshed = timelockService.refreshLockLeases(toRefresh);
            tokensToRefresh.removeAll(Sets.difference(toRefresh, refreshed));
        } catch (Throwable error) {
            log.warn("Error while refreshing locks. Trying again on next iteration", error);
        }
    }

    public void registerLock(LockToken token) {
        tokensToRefresh.add(token);
    }

    public void unregisterLocks(Collection<LockToken> tokens) {
        tokensToRefresh.removeAll(tokens);
    }
}
