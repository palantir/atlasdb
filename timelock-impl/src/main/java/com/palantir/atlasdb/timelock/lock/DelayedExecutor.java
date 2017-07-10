/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.palantir.common.time.Clock;

public class DelayedExecutor {

    private final ScheduledExecutorService executor;
    private final Clock clock;

    public DelayedExecutor(ScheduledExecutorService executor, Clock clock) {
        this.executor = executor;
        this.clock = clock;
    }

    public void runAt(Runnable task, Deadline deadline) {
        long delay = Math.max(0L, deadline.getMillisRemaining(clock));
        executor.schedule(
                task,
                delay,
                TimeUnit.MILLISECONDS);
    }

}
