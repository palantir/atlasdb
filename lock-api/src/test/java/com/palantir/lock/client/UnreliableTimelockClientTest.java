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

import static org.mockito.Mockito.verify;

import com.palantir.atlasdb.buggify.impl.NoOpBuggifyFactory;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.WaitForLocksRequest;
import java.util.Set;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class UnreliableTimelockClientTest {

    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.fromString("00000000-0000-0000-0000-000000000000"));
    private static final LockDescriptor LOCK_DESCRIPTOR = StringLockDescriptor.of("foo");
    private static final LockRequest LOCK_REQUEST = LockRequest.of(Set.of(LOCK_DESCRIPTOR), 1);

    @Mock
    private TimeLockClient timeLockClient;

    private UnreliableTimeLockService unreliableTimeLockService;

    @Before
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
}
