/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.UUID;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;

public class TargetedSweeperLockTest {
    private TimelockService mockLockService = mock(TimelockService.class);

    @Test
    public void successfulLockAndUnlock() throws InterruptedException {
        LockToken lockToken = LockToken.of(UUID.randomUUID());
        when(mockLockService.lock(any()))
                .thenReturn(() -> Optional.of(lockToken));
        Optional<TargetedSweeperLock> maybeLock = TargetedSweeperLock
                .tryAcquire(1, TableMetadataPersistence.SweepStrategy.CONSERVATIVE, mockLockService);

        assertThat(maybeLock).isPresent();
        TargetedSweeperLock lock = maybeLock.get();
        assertThat(lock.getShardAndStrategy()).isEqualTo(ShardAndStrategy.conservative(1));

        lock.unlock();
        verify(mockLockService, times(1)).unlock(ImmutableSet.of(lockToken));
        verify(mockLockService, times(1)).lock(any());
        verifyNoMoreInteractions(mockLockService);
    }

    @Test
    public void unsuccessfulLock() throws InterruptedException {
        when(mockLockService.lock(any())).thenReturn(() -> Optional.empty());
        Optional<TargetedSweeperLock> maybeLock = TargetedSweeperLock
                .tryAcquire(2, TableMetadataPersistence.SweepStrategy.THOROUGH, mockLockService);

        assertThat(maybeLock).isNotPresent();
        verify(mockLockService, times(1)).lock(any());
        verifyNoMoreInteractions(mockLockService);
    }
}
