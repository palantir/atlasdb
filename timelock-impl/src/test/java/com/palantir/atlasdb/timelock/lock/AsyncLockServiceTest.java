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

import com.palantir.atlasdb.timelock.lockwatches.BufferMetrics;
import com.palantir.atlasdb.timelock.timestampleases.TimestampLeaseMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.PTExecutors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;

public class AsyncLockServiceTest {

    @Test
    public void executorsShutDownAfterClose() {
        ScheduledExecutorService reaperExecutor = PTExecutors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService timeoutExecutor = PTExecutors.newSingleThreadScheduledExecutor();
        MetricsManager metricsManager = MetricsManagers.createForTests();
        AsyncLockService asyncLockService = AsyncLockService.createDefault(
                new LockLog(metricsManager.getRegistry(), () -> 1L),
                reaperExecutor,
                timeoutExecutor,
                BufferMetrics.of(metricsManager.getTaggedRegistry()),
                TimestampLeaseMetrics.of(metricsManager.getTaggedRegistry()));

        asyncLockService.close();
        assertThat(reaperExecutor.isShutdown()).isTrue();
        assertThat(timeoutExecutor.isShutdown()).isTrue();
    }
}
