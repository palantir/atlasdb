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
import static org.mockito.Mockito.when;

import java.math.BigInteger;

import org.junit.Test;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockService;

public class TargetedSweeperLockTest {
    private LockService mockLockService = mock(LockService.class);

    @Test
    public void successfulLockAndUnlock() throws InterruptedException {
        when(mockLockService.lock(any(), any()))
                .thenReturn(new LockRefreshToken(BigInteger.TEN, 10L));
        TargetedSweeperLock conservative = TargetedSweeperLock
                .tryAcquire(1, TableMetadataPersistence.SweepStrategy.CONSERVATIVE, mockLockService);
        assertThat(conservative.isHeld()).isTrue();
        assertThat(conservative.getShardAndStrategy()).isEqualTo(ShardAndStrategy.conservative(1));

        conservative.unlock();
        assertThat(conservative.isHeld()).isFalse();
    }

    @Test
    public void unsuccessfulLock() throws InterruptedException {
        when(mockLockService.lock(any(), any())).thenReturn(null);
        TargetedSweeperLock thorough = TargetedSweeperLock
                .tryAcquire(2, TableMetadataPersistence.SweepStrategy.THOROUGH, mockLockService);
        assertThat(thorough.isHeld()).isFalse();
        assertThat(thorough.getShardAndStrategy()).isEqualTo(ShardAndStrategy.thorough(2));
    }
}
