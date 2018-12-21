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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.ImmutableLockRequest;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;

/**
 * In this class, requests to the underlying timelock service get ids 0, 1, 2 and the returned lock tokens count
 * -1, -2, -3.
 */
@RunWith(MockitoJUnitRunner.class)
public final class PartitioningTimelockServiceTests {
    private static final int LOCK_BATCH_SIZE = 1;
    private static final int UNLOCK_BATCH_SIZE = 2;

    private final FakeClock clock = new FakeClock();

    @Mock private TimelockService timelock;

    private int locksGenerated = -1;
    private int requestIdsGenerated = 0;
    private TimelockService partitioning;

    @Before
    public void before() {
        partitioning = new PartitioningTimelockService(
                clock,
                timelock,
                () -> lt(locksGenerated--),
                () -> uuid(requestIdsGenerated++),
                LOCK_BATCH_SIZE,
                UNLOCK_BATCH_SIZE);
        when(timelock.unlock(any())).thenAnswer(inv -> inv.getArgument(0));
        when(timelock.lock(any())).thenAnswer(inv -> {
            LockRequest req = inv.getArgument(0);
            return succ(index(req.getRequestId()),
                    Iterables.getOnlyElement(req.getLockDescriptors()).getLockIdAsString().charAt(0)
            );
        });
    }

    @Test
    public void testRespectsNonBlockingDeadline() {
        when(timelock.lock(req(0, 0, "a")))
                .thenAnswer(inv -> {
                    clock.now = clock.now.plusMillis(10);
                    return succ(0, 'a');
                });
        assertThat(partitioning.lock(req(123, 0, "a", "b")))
                .isEqualTo(succ(-1));
    }

    @Test
    public void unlocksIfPartialFailureWhenLocking() {
        RuntimeException ex = new RuntimeException();
        when(timelock.lock(req(1, 0, "b"))).thenThrow(ex);
        assertThatThrownBy(() -> partitioning.lock(req(123, 0, "a", "b"))).isEqualTo(ex);
        verify(timelock).tryUnlock(ImmutableSet.of(lt(0, 'a')));
    }

    @Test
    public void unlocksIfClientSideTimeoutWhenLocking() {
        int deadline = 5;
        doAnswer(inv -> {
            clock.now = clock.now.plusMillis(deadline * 2);
            return succ(-1);
        }).when(timelock).lock(req(0, deadline, "a"));
        assertThat(partitioning.lock(req(123, deadline, "a", "b"))).isEqualTo(LockResponse.timedOut());
    }

    @Test
    public void unlocksIfServerSideTimeoutWhenLocking() {
        int deadline = 5;
        when(timelock.lock(req(0, deadline, "a"))).thenReturn(LockResponse.timedOut());
        assertThat(partitioning.lock(req(123, deadline, "a", "b"))).isEqualTo(LockResponse.timedOut());
    }

    @Test
    public void unlockingProxyTokenUnlocksTrueToken() {
        LockToken lock = partitioning.lock(req(123, 0, "a", "b", "c")).getToken();
        partitioning.unlock(ImmutableSet.of(lock));
        verify(timelock).unlock(ImmutableSet.of(lt(0, 'a'), lt(1, 'b')));
        verify(timelock).unlock(ImmutableSet.of(lt(2, 'c')));
    }

    @Test
    public void unlockingProxyTokenReturnsProxyTokenIfNecessary() {
        LockToken lock = partitioning.lock(req(123, 0, "a", "b", "c")).getToken();
        assertThat(timelock.unlock(ImmutableSet.of(lock))).containsExactly(lock);
    }

    @Test
    public void unlockingProxyTokenOmitsProxyTokenIfAnyCallerOmits() {
        doReturn(ImmutableSet.of()).when(timelock).unlock(ImmutableSet.of(lt(2, 'c')));
        LockToken lock = partitioning.lock(req(123, 0, "a", "b", "c")).getToken();
        assertThat(partitioning.unlock(ImmutableSet.of(lock))).isEmpty();
    }

    @Test
    public void tryUnlockProxyTokenUnlocksTrueToken() {
        LockToken lock = partitioning.lock(req(123, 0, "a", "b", "c")).getToken();
        partitioning.tryUnlock(ImmutableSet.of(lock));
        verify(timelock).tryUnlock(ImmutableSet.of(lt(0, 'a'), lt(1, 'b')));
        verify(timelock).tryUnlock(ImmutableSet.of(lt(2, 'c')));

    }

    @Test
    public void acquiresLocksInOrder() {
        InOrder inOrder = inOrder(timelock);
        partitioning.lock(req(123, 0, "c", "b", "a")).getToken();
        inOrder.verify(timelock).lock(req(0, 0, "a"));
        inOrder.verify(timelock).lock(req(1, 0, "b"));
        inOrder.verify(timelock).lock(req(2, 0, "c"));
    }

    @Test
    public void passesThroughInitialTokenWhenUnderMaximumSize() {
        when(timelock.lock(req(123, 0, "a"))).thenReturn(succ(-1));
        assertThat(partitioning.lock(req(123, 0, "a"))).isEqualTo(succ(-1));
    }

    private static final class FakeClock extends Clock {
        private Instant now = Instant.EPOCH;

        @Override
        public ZoneId getZone() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Clock withZone(ZoneId zone) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    private static UUID uuid(int index) {
        return new UUID(index, index);
    }

    private static int index(UUID uuid) {
        return (int) uuid.getLeastSignificantBits();
    }

    // String... should really be char... but there is no Arrays.stream(char...)
    private static LockRequest req(int id, int deadline, String... descriptors) {
        return ImmutableLockRequest.builder()
                .requestId(new UUID(id, id))
                .lockDescriptors(Arrays.stream(descriptors)
                        .map(StringLockDescriptor::of)
                        .collect(Collectors.toList()))
                .acquireTimeoutMs(deadline)
                .build();
    }

    private static LockResponse succ(int lt) {
        return LockResponse.successful(lt(lt));
    }

    private static LockResponse succ(int id, char descriptor) {
        return LockResponse.successful(lt(id, descriptor));
    }

    private static LockToken lt(int index) {
        return LockToken.of(new UUID(index, index));
    }

    private static LockToken lt(int id, char descriptor) {
        return LockToken.of(new UUID(descriptor, id));
    }
}
