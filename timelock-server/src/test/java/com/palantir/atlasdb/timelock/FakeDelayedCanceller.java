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

package com.palantir.atlasdb.timelock;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.timelock.lock.DelayedExecutor;
import com.palantir.common.time.Clock;

public class FakeDelayedCanceller extends DelayedExecutor {

    private Map<Long, Runnable> tasksByDeadline = Maps.newHashMap();
    private volatile boolean shouldCancelSynchronously = false;

    public FakeDelayedCanceller() {
        super(null, null);
    }

    protected FakeDelayedCanceller(ScheduledExecutorService executor,
            Clock clock) {
        super(executor, clock);
    }

    @Override
    public void runAt(Runnable task, long deadlineMs) {
        if (shouldCancelSynchronously) {
            task.run();
        } else {
            tasksByDeadline.put(deadlineMs, task);
        }
    }

    public void tick(long currentTime) {
        for (long deadline : tasksByDeadline.keySet()) {
            if (currentTime > deadline) {
                tasksByDeadline.remove(deadline).run();
            }
        }
    }

    public void setShouldCancelSynchronously(boolean shouldCancelSynchronously) {
        this.shouldCancelSynchronously = shouldCancelSynchronously;
    }
}
