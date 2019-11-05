/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import org.junit.Test;

import com.palantir.atlasdb.timelock.config.ImmutableRateLimitConfig;
import com.palantir.atlasdb.timelock.config.TargetedSweepLockControlConfig;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.PTExecutors;

public class AsyncLockServiceTest {
    private static final Supplier<TargetedSweepLockControlConfig.RateLimitConfig> RATE_LIMIT_CONFIG_SUPPLIER
            = () -> ImmutableRateLimitConfig.of(false,
            TargetedSweepLockControlConfig.ShardAndThreadConfig.defaultConfig());

    @Test
    public void executorsShutDownAfterClose() {
        ScheduledExecutorService reaperExecutor = PTExecutors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService timeoutExecutor = PTExecutors.newSingleThreadScheduledExecutor();
        AsyncLockService asyncLockService = AsyncLockService.createDefault(
                new LockLog(MetricsManagers.createForTests().getRegistry(), () -> 1L),
                reaperExecutor,
                timeoutExecutor,
                RATE_LIMIT_CONFIG_SUPPLIER,
                new LockWatchingServiceImpl(() -> 1L));

        asyncLockService.close();
        assertThat(reaperExecutor.isShutdown()).isTrue();
        assertThat(timeoutExecutor.isShutdown()).isTrue();
    }
}
