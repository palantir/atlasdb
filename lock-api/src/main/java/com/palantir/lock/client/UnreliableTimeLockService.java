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
import com.palantir.atlasdb.buggify.api.BuggifyFactory;
import com.palantir.atlasdb.buggify.impl.DefaultBuggifyFactory;
import com.palantir.atlasdb.common.api.annotations.ReviewedRestrictedApiUsage;
import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.TimestampLeaseResults;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.TimestampRange;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A version of timelock service that has a chance to randomly lose locks during refresh or immediately after
 * acquiring, or randomly fast forwarding timestamps every time we get a new timestamp.
 * This is useful for testing the behavior of clients when locks are lost or timestamps randomly jump forward.
 */
public final class UnreliableTimeLockService implements TimelockService {
    private static final SafeLogger log = SafeLoggerFactory.get(UnreliableTimeLockService.class);
    private static final double UNLOCK_PROBABILITY = 0.05;
    private static final double INCREASE_TIMESTAMP_PROBABILITY = 0.1;
    private static final double FAIL_TO_REFRESH_PROBABILITY = 0.01;

    private final TimelockService delegate;
    private final BuggifyFactory buggify;
    private final RandomizedTimestampManager timestampManager;

    public static UnreliableTimeLockService create(
            TimelockService timelockService, RandomizedTimestampManager timestampManager) {
        return new UnreliableTimeLockService(timelockService, timestampManager, DefaultBuggifyFactory.INSTANCE);
    }

    @VisibleForTesting
    UnreliableTimeLockService(
            TimelockService delegate, RandomizedTimestampManager timestampManager, BuggifyFactory buggifyFactory) {
        this.delegate = delegate;
        this.buggify = buggifyFactory;
        this.timestampManager = timestampManager;
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        maybeRandomlyIncreaseTimestamp();
        return timestampManager.getFreshTimestamp();
    }

    @Override
    public long getCommitTimestamp(long startTs, LockToken commitLocksToken) {
        maybeRandomlyIncreaseTimestamp();
        return timestampManager.getCommitTimestamp(startTs, commitLocksToken);
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        maybeRandomlyIncreaseTimestamp();
        return timestampManager.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp() {
        return delegate.lockImmutableTimestamp();
    }

    @Override
    public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        return delegate.startIdentifiedAtlasDbTransactionBatch(count);
    }

    @Override
    public long getImmutableTimestamp() {
        return delegate.getImmutableTimestamp();
    }

    @Override
    public LockResponse lock(LockRequest request) {
        LockResponse response = delegate.lock(request);
        buggify.maybe(UNLOCK_PROBABILITY).run(() -> {
            log.info("BUGGIFY: Unlocking lock token {} after acquiring", SafeArg.of("token", response.getToken()));
            delegate.unlock(Set.of(response.getToken()));
        });
        return response;
    }

    @Override
    public LockResponse lock(LockRequest lockRequest, ClientLockingOptions options) {
        LockResponse response = delegate.lock(lockRequest, options);
        buggify.maybe(UNLOCK_PROBABILITY).run(() -> {
            log.info("BUGGIFY: Unlocking lock token {} after acquiring", SafeArg.of("token", response.getToken()));
            delegate.unlock(Set.of(response.getToken()));
        });
        return response;
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return delegate.waitForLocks(request);
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        Set<LockToken> tokensToRefresh = tokens.stream()
                .filter(_token -> !buggify.maybe(FAIL_TO_REFRESH_PROBABILITY).asBoolean())
                .collect(Collectors.toSet());
        Set<LockToken> tokensToUnlock = Sets.difference(tokens, tokensToRefresh);
        if (!tokensToUnlock.isEmpty()) {
            log.info("BUGGIFY: Unlocking tokens on refresh: {}", SafeArg.of("tokens", tokensToUnlock));
            delegate.unlock(tokensToUnlock);
        }
        return delegate.refreshLockLeases(tokensToRefresh);
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return delegate.unlock(tokens);
    }

    @Override
    public void tryUnlock(Set<LockToken> tokens) {
        delegate.tryUnlock(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }

    @ReviewedRestrictedApiUsage
    @Override
    public TimestampLeaseResults acquireTimestampLeases(Map<TimestampLeaseName, Integer> requests) {
        return delegate.acquireTimestampLeases(requests);
    }

    @ReviewedRestrictedApiUsage
    @Override
    public Map<TimestampLeaseName, Long> getMinLeasedTimestamps(Set<TimestampLeaseName> timestampNames) {
        return delegate.getMinLeasedTimestamps(timestampNames);
    }

    private void maybeRandomlyIncreaseTimestamp() {
        buggify.maybe(INCREASE_TIMESTAMP_PROBABILITY).run(timestampManager::randomlyIncreaseTimestamp);
    }
}
