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
package com.palantir.leader;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.common.concurrent.PTExecutors;

public class AsyncLeadershipObserver implements LeadershipObserver {

    private final ExecutorService executorService;
    private final List<Runnable> leaderTasks = new CopyOnWriteArrayList<>();
    private final List<Runnable> followerTasks = new CopyOnWriteArrayList<>();

    public static AsyncLeadershipObserver create() {
        /**
         * This executor should be kept single threaded to ensure ordered execution of tasks
         * which is guaranteed by the API contract of {@link LeadershipObserver}
         */
        return new AsyncLeadershipObserver(PTExecutors.newSingleThreadExecutor(true));
    }

    public static AsyncLeadershipObserver create(Runnable gainedLeadershipTask, Runnable lostLeadershipTask) {
        AsyncLeadershipObserver asyncLeadershipObserver = create();
        asyncLeadershipObserver.executeWhenGainedLeadership(gainedLeadershipTask);
        asyncLeadershipObserver.executeWhenLostLeadership(lostLeadershipTask);
        return asyncLeadershipObserver;
    }

    @VisibleForTesting
    AsyncLeadershipObserver(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public synchronized void gainedLeadership() {
        executorService.execute(() -> leaderTasks.forEach(Runnable::run));
    }

    @Override
    public synchronized void lostLeadership() {
        executorService.execute(() -> followerTasks.forEach(Runnable::run));
    }

    @Override
    public synchronized void executeWhenGainedLeadership(Runnable task) {
        leaderTasks.add(task);
    }

    @Override
    public synchronized void executeWhenLostLeadership(Runnable task) {
        followerTasks.add(task);
    }
}
