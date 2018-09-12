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

package com.palantir.leader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.palantir.common.concurrent.PTExecutors;

public class AsyncLeadershipObserver implements LeadershipObserver {

    private ExecutorService executorService = PTExecutors.newSingleThreadExecutor();
    private List<Runnable> leaderTasks = new ArrayList<>();
    private List<Runnable> followerTasks = new ArrayList<>();

    @Override
    public void gainedLeadership() {
        executorService.execute(this::executeLeaderTasks);
    }

    @Override
    public void lostLeadership() {
        executorService.execute(this::executeFollowerTasks);
    }

    @Override
    public synchronized void executeWhenGainedLeadership(Runnable task) {
        leaderTasks.add(task);
    }

    @Override
    public synchronized void executeWhenLostLeadership(Runnable task) {
        followerTasks.add(task);
    }

    private void executeFollowerTasks() {
        followerTasks.forEach(Runnable::run);
    }

    private void executeLeaderTasks() {
        leaderTasks.forEach(Runnable::run);
    }
}
