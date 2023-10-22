/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.buggify.api.BuggifyFactory;
import com.palantir.atlasdb.buggify.impl.DefaultBuggify;
import com.palantir.atlasdb.buggify.impl.NoOpBuggify;
import com.palantir.atlasdb.buggify.impl.NoOpBuggifyFactory;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.WaitForLocksRequest;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public final class UnreliableTimelockClientTest {

    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.fromString("00000000-0000-0000-0000-000000000000"));

    private static final LockToken LOCK_TOKEN_2 = LockToken.of(UUID.fromString("00000000-0000-0000-0000-000000000001"));
    private static final LockDescriptor LOCK_DESCRIPTOR = StringLockDescriptor.of("foo");
    private static final LockRequest LOCK_REQUEST = LockRequest.of(Set.of(LOCK_DESCRIPTOR), 1);

    @Mock
    private TimeLockClient timeLockClient;

    private UnreliableTimeLockService unreliableTimeLockService;

    @BeforeEach
    public void before() {
        unreliableTimeLockService = new UnreliableTimeLockService(timeLockClient, NoOpBuggifyFactory.INSTANCE);
    }

    @Test
    public void verifyIsInitializedCallsDelegate() {
        unreliableTimeLockService.isInitialized();
        verify(timeLockClient).isInitialized();
    }

    @Test
    public void verifyGetFreshTimestampCallsDelegate() {
        unreliableTimeLockService.getFreshTimestamp();
        verify(timeLockClient).getFreshTimestamp();
    }

    @Test
    public void verifyGetCommitTimestampsCallsDelegate() {
        unreliableTimeLockService.getCommitTimestamp(1, LOCK_TOKEN);
        verify(timeLockClient).getCommitTimestamp(1, LOCK_TOKEN);
    }

    @Test
    public void verifyGetFreshTimestampsCallsDelegate() {
        unreliableTimeLockService.getFreshTimestamps(1);
        verify(timeLockClient).getFreshTimestamps(1);
    }

    @Test
    public void verifyLockImmutableTimestampCallsDelegate() {
        unreliableTimeLockService.lockImmutableTimestamp();
        verify(timeLockClient).lockImmutableTimestamp();
    }

    @Test
    public void verifyStartIdentifiedAtlasDbTransactionCallsDelegate() {
        unreliableTimeLockService.startIdentifiedAtlasDbTransactionBatch(1);
        verify(timeLockClient).startIdentifiedAtlasDbTransactionBatch(1);
    }

    @Test
    public void verifyGetImmutableTimestampCallsDelegate() {
        unreliableTimeLockService.getImmutableTimestamp();
        verify(timeLockClient).getImmutableTimestamp();
    }

    @Test
    public void verifyLockCallsDelegate() {
        unreliableTimeLockService.lock(LOCK_REQUEST);
        verify(timeLockClient).lock(LOCK_REQUEST);
    }

    @Test
    public void lockMayUnlockImmediately() {
        when(timeLockClient.lock(LOCK_REQUEST)).thenReturn(LockResponse.successful(LOCK_TOKEN));
        new UnreliableTimeLockService(timeLockClient, createAlwaysBuggyFactory()).lock(LOCK_REQUEST);
        verify(timeLockClient).unlock(Set.of(LOCK_TOKEN));
    }

    @Test
    public void lockWithOptionMayUnlockImmediately() {
        ClientLockingOptions lockingOptions = ClientLockingOptions.getDefault();
        when(timeLockClient.lock(LOCK_REQUEST, lockingOptions)).thenReturn(LockResponse.successful(LOCK_TOKEN));
        new UnreliableTimeLockService(timeLockClient, createAlwaysBuggyFactory()).lock(LOCK_REQUEST, lockingOptions);
        verify(timeLockClient).unlock(Set.of(LOCK_TOKEN));
    }

    @Test
    public void verifyLockWithOptionsCallsDelegate() {
        unreliableTimeLockService.lock(LOCK_REQUEST, ClientLockingOptions.getDefault());
        verify(timeLockClient).lock(LOCK_REQUEST, ClientLockingOptions.getDefault());
    }

    @Test
    public void verifyWaitForLocksCallsDelegate() {
        WaitForLocksRequest waitForLocksRequest = WaitForLocksRequest.of(Set.of(LOCK_DESCRIPTOR), 1);
        unreliableTimeLockService.waitForLocks(waitForLocksRequest);
        verify(timeLockClient).waitForLocks(waitForLocksRequest);
    }

    @Test
    public void refreshLockLeasesMaybeUnlocksLocks() {
        BuggifyFactory factory = mock(BuggifyFactory.class);
        when(factory.maybe(anyDouble())).thenReturn(DefaultBuggify.INSTANCE, NoOpBuggify.INSTANCE);
        new UnreliableTimeLockService(timeLockClient, factory)
                .refreshLockLeases(new LinkedHashSet<>(List.of(LOCK_TOKEN, LOCK_TOKEN_2)));
        verify(timeLockClient).refreshLockLeases(Set.of(LOCK_TOKEN_2));
        verify(timeLockClient).unlock(Set.of(LOCK_TOKEN));
    }

    @Test
    public void verifyRefreshLockLeasesCallsDelegate() {
        unreliableTimeLockService.refreshLockLeases(Set.of(LOCK_TOKEN));
        verify(timeLockClient).refreshLockLeases(Set.of(LOCK_TOKEN));
    }

    @Test
    public void verifyUnlockCallsDelegate() {
        unreliableTimeLockService.unlock(Set.of(LOCK_TOKEN));
        verify(timeLockClient).unlock(Set.of(LOCK_TOKEN));
    }

    @Test
    public void verifyTryUnlockCallsDelegate() {
        unreliableTimeLockService.tryUnlock(Set.of(LOCK_TOKEN));
        verify(timeLockClient).tryUnlock(Set.of(LOCK_TOKEN));
    }

    @Test
    public void verifyCurrentTimeMillisCallsDelegate() {
        unreliableTimeLockService.currentTimeMillis();
        verify(timeLockClient).currentTimeMillis();
    }

    private static BuggifyFactory createAlwaysBuggyFactory() {
        BuggifyFactory factory = mock(BuggifyFactory.class);
        when(factory.maybe(anyDouble())).thenReturn(DefaultBuggify.INSTANCE);
        return factory;
    }
}
