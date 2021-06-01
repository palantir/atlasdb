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
package com.palantir.lock.impl;

import com.google.common.collect.ImmutableSortedMap;
import com.palantir.common.base.Throwables;
import com.palantir.lock.AtlasTimestampLockDescriptor;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.TimestampAndPartition;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A {@link TimelockService} implementation that delegates to a {@link LockService} and {@link TimestampService}.
 */
public class LegacyTimelockService implements TimelockService {

    private final TimestampService timestampService;
    private final LockService lockService;
    private final LockClient immutableTsLockClient;

    public LegacyTimelockService(
            TimestampService timestampService, LockService lockService, LockClient immutableTsLockClient) {
        this.timestampService = timestampService;
        this.lockService = lockService;
        this.immutableTsLockClient = immutableTsLockClient;
    }

    @Override
    public boolean isInitialized() {
        return timestampService.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        return timestampService.getFreshTimestamp();
    }

    @Override
    public long getCommitTimestamp(long startTs, LockToken commitLocksToken) {
        return getFreshTimestamp();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return timestampService.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp() {
        long immutableLockTs = timestampService.getFreshTimestamp();
        LockDescriptor lockDesc = AtlasTimestampLockDescriptor.of(immutableLockTs);
        com.palantir.lock.LockRequest lockRequest = com.palantir.lock.LockRequest.builder(
                        ImmutableSortedMap.of(lockDesc, LockMode.READ))
                .withLockedInVersionId(immutableLockTs)
                .build();
        LockRefreshToken lock;

        try {
            lock = lockService.lock(immutableTsLockClient.getClientId(), lockRequest);
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }

        try {
            return LockImmutableTimestampResponse.of(
                    getImmutableTimestampInternal(immutableLockTs), LockTokenConverter.toTokenV2(lock));
        } catch (Throwable e) {
            if (lock != null) {
                try {
                    lockService.unlock(lock);
                } catch (Throwable unlockThrowable) {
                    e.addSuppressed(unlockThrowable);
                }
            }
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    @Override
    public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        // Track these separately in the case that getFreshTimestamp fails but lockImmutableTimestamp succeeds
        List<LockImmutableTimestampResponse> immutableTimestampLocks = new ArrayList<>();
        List<StartIdentifiedAtlasDbTransactionResponse> responses = new ArrayList<>();
        try {
            IntStream.range(0, count).forEach($ -> {
                LockImmutableTimestampResponse immutableTimestamp = lockImmutableTimestamp();
                immutableTimestampLocks.add(immutableTimestamp);
                responses.add(StartIdentifiedAtlasDbTransactionResponse.of(
                        immutableTimestamp, TimestampAndPartition.of(getFreshTimestamp(), 0)));
            });
            return responses;
        } catch (RuntimeException | Error throwable) {
            try {
                unlock(immutableTimestampLocks.stream()
                        .map(LockImmutableTimestampResponse::getLock)
                        .collect(Collectors.toSet()));
            } catch (Throwable unlockThrowable) {
                throwable.addSuppressed(unlockThrowable);
            }
            throw throwable;
        }
    }

    @Override
    public long getImmutableTimestamp() {
        long ts = timestampService.getFreshTimestamp();
        return getImmutableTimestampInternal(ts);
    }

    @Override
    public LockResponse lock(LockRequest request) {
        LockRefreshToken legacyToken = lockAnonymous(toLegacyLockRequest(request));
        if (legacyToken == null) {
            return LockResponse.timedOut();
        } else {
            return LockResponse.successful(LockTokenConverter.toTokenV2(legacyToken));
        }
    }

    @Override
    public LockResponse lock(LockRequest lockRequest, ClientLockingOptions options) {
        return lock(lockRequest);
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        com.palantir.lock.LockRequest legacyRequest = toLegacyWaitForLocksRequest(request.getLockDescriptors());

        // this blocks indefinitely, and can only fail if the connection fails (and throws an exception)
        lockAnonymous(legacyRequest);
        return WaitForLocksResponse.successful();
    }

    private com.palantir.lock.LockRequest toLegacyLockRequest(LockRequest request) {
        SortedMap<LockDescriptor, LockMode> locks = buildLockMap(request.getLockDescriptors(), LockMode.WRITE);
        return com.palantir.lock.LockRequest.builder(locks)
                .blockForAtMost(SimpleTimeDuration.of(request.getAcquireTimeoutMs(), TimeUnit.MILLISECONDS))
                .build();
    }

    private com.palantir.lock.LockRequest toLegacyWaitForLocksRequest(Set<LockDescriptor> lockDescriptors) {
        SortedMap<LockDescriptor, LockMode> locks = buildLockMap(lockDescriptors, LockMode.READ);
        return com.palantir.lock.LockRequest.builder(locks).lockAndRelease().build();
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        Set<LockRefreshToken> refreshTokens =
                tokens.stream().map(LockTokenConverter::toLegacyToken).collect(Collectors.toSet());
        return lockService.refreshLockRefreshTokens(refreshTokens).stream()
                .map(LockTokenConverter::toTokenV2)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        Set<LockToken> unlocked = new HashSet<>();
        for (LockToken tokenV2 : tokens) {
            LockRefreshToken legacyToken = LockTokenConverter.toLegacyToken(tokenV2);
            if (lockService.unlock(legacyToken)) {
                unlocked.add(tokenV2);
            }
        }
        return unlocked;
    }

    @Override
    public void tryUnlock(Set<LockToken> tokens) {
        unlock(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return lockService.currentTimeMillis();
    }

    private long getImmutableTimestampInternal(long ts) {
        Long minLocked = lockService.getMinLockedInVersionId(immutableTsLockClient.getClientId());
        return minLocked == null ? ts : minLocked;
    }

    private LockRefreshToken lockAnonymous(com.palantir.lock.LockRequest request) {
        try {
            return lockService.lock(LockClient.ANONYMOUS.getClientId(), request);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
    }

    private SortedMap<LockDescriptor, LockMode> buildLockMap(Set<LockDescriptor> lockDescriptors, LockMode lockMode) {
        SortedMap<LockDescriptor, LockMode> locks = new TreeMap<>();
        for (LockDescriptor descriptor : lockDescriptors) {
            locks.put(descriptor, lockMode);
        }
        return locks;
    }
}
