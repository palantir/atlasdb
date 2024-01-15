/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.store;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import com.palantir.lock.client.TimestampManager;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.TimestampRange;
import java.security.SecureRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class UnreliableTimestampManager implements TimestampManager {
    private static final SafeLogger log = SafeLoggerFactory.get(UnreliableTimestampManager.class);

    // A partition is sized at 2^23 timestamps, and we want to skip through potentially many at a time.
    private static final long THOUSAND_COARSE_PARTITIONS_SIZE = 1000 * (1L << 23);
    private static final SecureRandom SECURE_RANDOM = DefaultNativeSamplingSecureRandomFactory.INSTANCE.create();
    private final ReadWriteLock timestampLock;
    private final TimelockService delegate;

    private final LongSupplier getFreshTimestamp;
    private final LongConsumer fastForwardTimestamp;

    static UnreliableTimestampManager create(TimelockService timelockService, LongConsumer randomlyIncreaseTimestamp) {
        return new UnreliableTimestampManager(
                timelockService, timelockService::getFreshTimestamp, randomlyIncreaseTimestamp);
    }

    @VisibleForTesting
    static UnreliableTimestampManager create(
            TimelockService timelockService, LongSupplier getFreshTimestamp, LongConsumer fastForwardTimestamp) {
        return new UnreliableTimestampManager(timelockService, getFreshTimestamp, fastForwardTimestamp);
    }

    private UnreliableTimestampManager(
            TimelockService delegate, LongSupplier getFreshTimestamp, LongConsumer fastForwardTimestamp) {
        // Fast forward doesn't actually verify it's going forward, so we need to make sure we don't end up
        // with a situation where we buggify -> getFreshTimestamp -> calculate next timestamp -> get stuck
        // while the timestamp progresses, then the original thread then fast forwards (but actually goes back in time)
        // It's an unfair lock to add more chaos to the buggified requests.
        timestampLock = new ReentrantReadWriteLock(false);
        this.delegate = delegate;
        this.getFreshTimestamp = getFreshTimestamp;
        this.fastForwardTimestamp = fastForwardTimestamp;
    }

    @Override
    public void randomlyIncreaseTimestamp() {
        runWithWriteLock(() -> {
            long currentTimestamp = getFreshTimestamp.getAsLong();
            long newTimestamp = SECURE_RANDOM
                    .longs(1, currentTimestamp + 1, currentTimestamp + THOUSAND_COARSE_PARTITIONS_SIZE)
                    .findFirst()
                    .getAsLong();
            log.info(
                    "BUGGIFY: Increasing timestamp from {} to {}",
                    SafeArg.of("currentTimestamp", currentTimestamp),
                    SafeArg.of("newTimestamp", newTimestamp));
            fastForwardTimestamp.accept(newTimestamp);
            return null;
        });
    }

    @Override
    public long getFreshTimestamp() {
        return runWithReadLock(delegate::getFreshTimestamp);
    }

    @Override
    public long getCommitTimestamp(long startTs, LockToken commitLocksToken) {
        return runWithReadLock(() -> delegate.getCommitTimestamp(startTs, commitLocksToken));
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return runWithReadLock(() -> delegate.getFreshTimestamps(numTimestampsRequested));
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
