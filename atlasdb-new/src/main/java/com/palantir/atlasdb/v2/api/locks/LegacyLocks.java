/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.locks;

import static com.palantir.logsafe.Preconditions.checkState;

import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AsyncCallable;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.timelock.lock.AsyncLock;
import com.palantir.atlasdb.timelock.lock.AsyncResult;
import com.palantir.atlasdb.timelock.lock.HeldLocks;
import com.palantir.atlasdb.timelock.lock.ImmutableTimestampTracker;
import com.palantir.atlasdb.timelock.lock.LeaderClock;
import com.palantir.atlasdb.timelock.lock.LockAcquirer;
import com.palantir.atlasdb.timelock.lock.LockCollection;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.lock.OrderedLocks;
import com.palantir.atlasdb.timelock.lock.TimeLimit;
import com.palantir.atlasdb.timelock.lock.watch.LockWatchingService;
import com.palantir.atlasdb.timelock.lock.watch.ValueAndVersion;
import com.palantir.atlasdb.v2.api.NewIds;
import com.palantir.atlasdb.v2.api.NewIds.Row;
import com.palantir.atlasdb.v2.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.timestamps.HeldImmutableLock;
import com.palantir.atlasdb.v2.api.timestamps.Timestamps;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.AtlasTimestampLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchRequest;
import com.palantir.lock.watch.LockWatchStateUpdate;

public class LegacyLocks implements NewLocks, Timestamps {
    private static final LockLog lockLog = new LockLog(new MetricRegistry(), () -> 123456789L);
    private static final TimeLimit TIMEOUT = TimeLimit.of(10_000);

    private final LockCollection locks = new LockCollection();
    private final ScheduledExecutorService executor;
    private final LockAcquirer lockAcquirer;
    private final ImmutableTimestampTracker immutableTimestamp = new ImmutableTimestampTracker();

    private long timestamp = 0;

    public LegacyLocks(ScheduledExecutorService executor, LeaderClock leaderClock) {
        lockAcquirer = new LockAcquirer(lockLog, executor, leaderClock, FakeLockWatchingService.INSTANCE);
        this.executor = executor;
    }

    private ListenableFuture<?> run(Runnable runnable) {
        return call(() -> {
            runnable.run();
            return null;
        });
    }

    private <T> ListenableFuture<T> call(Callable<T> callable) {
        return callAsync(() -> Futures.immediateFuture(callable.call()));
    }

    private <T> ListenableFuture<T> callAsync(AsyncCallable<T> callable) {
        return Futures.submitAsync(callable, executor);
    }

    @Override
    public ListenableFuture<NewLockToken> lock(Set<NewLockDescriptor> descriptors) {
        OrderedLocks orderedLocks = locks.getAll(toLegacy(descriptors));
        return acquire(orderedLocks);
    }

    private ListenableFuture<NewLockToken> acquire(OrderedLocks orderedLocks) {
        return callAsync(() -> {
            UUID requestId = UUID.randomUUID();
            return Futures.transform(toListenableFuture(lockAcquirer.acquireLocks(requestId, orderedLocks, TIMEOUT)),
                    LegacyLockToken::of, MoreExecutors.directExecutor());
        });
    }

    @Override
    public ListenableFuture<?> await(Set<NewLockDescriptor> descriptors) {
        OrderedLocks orderedLocks = locks.getAll(toLegacy(descriptors));
        UUID requestId = UUID.randomUUID();
        return toListenableFuture(lockAcquirer.waitForLocks(requestId, orderedLocks, TIMEOUT));
    }

    @Override
    public ListenableFuture<?> checkStillValid(Set<NewLockToken> lockTokens) {
        return run(() -> {
            checkState(lockTokens.stream()
                    .map(LegacyLockToken.class::cast)
                    .map(LegacyLockToken::locks)
                    .allMatch(HeldLocks::refresh));
        });
    }

    @Override
    public void unlock(Set<NewLockToken> lockTokens) {
        lockTokens.stream().map(LegacyLockToken.class::cast)
                .map(LegacyLockToken::locks)
                .forEach(HeldLocks::unlockExplicitly);
    }

    private static Set<LockDescriptor> toLegacy(Set<NewLockDescriptor> descriptors) {
        return descriptors.stream().map(LegacyLocks::toLegacy).collect(Collectors.toSet());
    }

    private static LockDescriptor toLegacy(NewLockDescriptor descriptor) {
        return descriptor.accept(new NewLockDescriptor.Visitor<LockDescriptor>() {
            @Override
            public LockDescriptor timestamp(long timestamp) {
                return AtlasTimestampLockDescriptor.of(timestamp);
            }

            @Override
            public LockDescriptor cell(Table table, NewIds.Cell cell) {
                return AtlasCellLockDescriptor.of(
                        table.getName(), cell.row().toByteArray(), cell.column().toByteArray());
            }

            @Override
            public LockDescriptor row(Table table, Row row) {
                return AtlasRowLockDescriptor.of(table.getName(), row.toByteArray());
            }
        });
    }

    @Override
    public ListenableFuture<Long> getStartTimestamp() {
        return getFreshTimestamp();
    }

    @Override
    public ListenableFuture<HeldImmutableLock> lockImmutableTs() {
        long ts = timestamp++;
        AsyncLock lock = immutableTimestamp.getLockFor(timestamp);
        return Futures.transform(acquire(OrderedLocks.fromSingleLock(lock)),
                acquiredLock -> HeldImmutableLock.of(
                        acquiredLock, immutableTimestamp.getImmutableTimestamp().orElse(ts)),
                executor);
    }

    @Override
    public ListenableFuture<Long> getCommitTimestamp() {
        return getFreshTimestamp();
    }

    private ListenableFuture<Long> getFreshTimestamp() {
        return callAsync(() -> {
            long ts = timestamp++;
            return call(() -> ts);
        });
    }

    @Value.Immutable
    interface LegacyLockToken extends NewLockToken {
        @Value.Parameter
        HeldLocks locks();

        static NewLockToken of(HeldLocks token) {
            return ImmutableLegacyLockToken.of(token);
        }
    }

    private static <T> ListenableFuture<T> toListenableFuture(AsyncResult<T> asyncResult) {
        SettableFuture<T> result = SettableFuture.create();
        asyncResult.onComplete(() -> {
            if (asyncResult.isFailed()) {
                result.setException(asyncResult.getError());
            } else if (asyncResult.isTimedOut()) {
                result.setException(new TimeoutException()); // eh
            } else {
                result.set(asyncResult.get());
            }
        });
        return result;
    }

    private enum FakeLockWatchingService implements LockWatchingService {
        INSTANCE;

        @Override
        public void startWatching(LockWatchRequest locksToWatch) {

        }

        @Override
        public LockWatchStateUpdate getWatchStateUpdate(OptionalLong lastKnownVersion) {
            return null;
        }

        @Override
        public LockWatchStateUpdate getWatchStateUpdate(OptionalLong lastKnownVersion, long endVersion) {
            return null;
        }

        @Override
        public <T> ValueAndVersion<T> runTaskAndAtomicallyReturnLockWatchVersion(Supplier<T> task) {
            return null;
        }

        @Override
        public void registerLock(Set<LockDescriptor> locksTakenOut, LockToken token) {

        }

        @Override
        public void registerUnlock(Set<LockDescriptor> locksUnlocked) {

        }
    }
}
