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
import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampRange;
import java.security.SecureRandom;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A version of timelock service that has a chance to randomly lose locks during refresh or immediately after
 * acquiring. This is useful for testing the behavior of clients when locks are lost.
 */
public final class UnreliableTimeLockService implements TimelockService {
    // A partition is sized at 2^23 timestamps, and we want to skip through potentially many at a time.
    private static final long THOUSAND_COARSE_PARTITIONS_SIZE = 1000 * (1L << 23);
    private static final SecureRandom SECURE_RANDOM = DefaultNativeSamplingSecureRandomFactory.INSTANCE.create();

    private static final SafeLogger log = SafeLoggerFactory.get(UnreliableTimeLockService.class);

    private final TimelockService delegate;
    private final TimestampManagementService managementService;
    private final BuggifyFactory buggify;
    private final ReadWriteLock timestampLock;

    public static UnreliableTimeLockService create(
            TimelockService timelockService, TimestampManagementService managementService) {
        return new UnreliableTimeLockService(timelockService, managementService, DefaultBuggifyFactory.INSTANCE);
    }

    @VisibleForTesting
    UnreliableTimeLockService(
            TimelockService delegate,
            TimestampManagementService timeLockManagementService,
            BuggifyFactory buggifyFactory) {
        this.delegate = delegate;
        this.managementService = timeLockManagementService;
        this.buggify = buggifyFactory;

        // Fast forward doesn't actually verify it's going forward, so we need to make sure we don't end up
        // with a situation where we buggify -> getFreshTimestamp -> calculate next timestamp -> get stuck
        // while the timestamp progresses, then the original thread then fast forwards (but actually goes back in time)
        // It's an unfair lock to add more chaos to the buggified requests.
        timestampLock = new ReentrantReadWriteLock(false);
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        maybeRandomlyIncreaseTimestamp();
        return runWithReadLock(delegate::getFreshTimestamp);
    }

    @Override
    public long getCommitTimestamp(long startTs, LockToken commitLocksToken) {
        maybeRandomlyIncreaseTimestamp();
        return runWithReadLock(() -> delegate.getCommitTimestamp(startTs, commitLocksToken));
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        maybeRandomlyIncreaseTimestamp();
        return runWithReadLock(() -> delegate.getFreshTimestamps(numTimestampsRequested));
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
        buggify.maybe(0.05).run(() -> {
            log.info("BUGGIFY: Unlocking lock token {} after acquiring", SafeArg.of("token", response.getToken()));
            delegate.unlock(Set.of(response.getToken()));
        });
        return response;
    }

    @Override
    public LockResponse lock(LockRequest lockRequest, ClientLockingOptions options) {
        LockResponse response = delegate.lock(lockRequest, options);
        buggify.maybe(0.05).run(() -> {
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
                .filter(_token -> !buggify.maybe(0.01).asBoolean())
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

    private void maybeRandomlyIncreaseTimestamp() {
        buggify.maybe(0.1).run(this::randomlyIncreaseTimestamp);
    }

    private void randomlyIncreaseTimestamp() {
        runWithWriteLock(() -> {
            long currentTimestamp = delegate.getFreshTimestamp();
            long newTimestamp = SECURE_RANDOM
                    .longs(1, currentTimestamp + 1, currentTimestamp + THOUSAND_COARSE_PARTITIONS_SIZE)
                    .findFirst()
                    .getAsLong();
            log.info(
                    "BUGGIFY: Increasing timestamp from {} to {}",
                    SafeArg.of("currentTimestamp", currentTimestamp),
                    SafeArg.of("newTimestamp", newTimestamp));
            managementService.fastForwardTimestamp(newTimestamp);
            return null;
        });
    }

    private <T> T runWithReadLock(Supplier<T> task) {
        Lock lock = timestampLock.readLock();
        lock.lock();
        try {
            return task.get();
        } finally {
            lock.unlock();
        }
    }

    private <T> T runWithWriteLock(Supplier<T> task) {
        Lock lock = timestampLock.writeLock();
        lock.lock();

        try {
            return task.get();
        } finally {
            lock.unlock();
        }
    }
}
