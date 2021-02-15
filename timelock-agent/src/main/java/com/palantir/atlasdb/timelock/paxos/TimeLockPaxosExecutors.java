/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.concurrent.PTExecutors;
import java.util.Map;
import java.util.concurrent.ExecutorService;

final class TimeLockPaxosExecutors {
    /**
     * The size of the thread pool used for remote calls to each TimeLock remote (and, thus, a limiter on the number of
     * concurrent requests that can be made to each remote).
     *
     * This number was chosen based on analysing past loads on internal metrics platform. It was selected to permit
     * most instances where a spike in executor tasks was successfully serviced and the system recovered thereafter.
     * Choosing too large of a value leads to an unnecessary build up of threads when an individual node is slow;
     * choosing too small of a value may lead to unnecessary leader elections or add overhead to the Paxos protocol.
     */
    static final int MAXIMUM_POOL_SIZE = 384;

    private TimeLockPaxosExecutors() {
        // no
    }

    /**
     * Creates a mapping of services to {@link ExecutorService}s indicating that tasks oriented towards the relevant
     * node should be run on the associated executor.
     *
     * It is assumed that tasks run on the local node will return quickly (hence the use of the direct executor).
     */
    static <T> Map<T, CheckedRejectionExecutorService> createBoundedExecutors(
            int poolSize, LocalAndRemotes<T> localAndRemotes, String useCase) {
        int numRemotes = localAndRemotes.remotes().size();
        ImmutableMap.Builder<T, CheckedRejectionExecutorService> remoteExecutors =
                ImmutableMap.builderWithExpectedSize(numRemotes);
        for (int index = 0; index < numRemotes; index++) {
            T remote = localAndRemotes.remotes().get(index);
            remoteExecutors.put(remote, createBoundedExecutor(poolSize, useCase, index));
        }
        remoteExecutors.put(
                localAndRemotes.local(), new CheckedRejectionExecutorService(MoreExecutors.newDirectExecutorService()));
        return remoteExecutors.build();
    }

    /**
     * Creates a bounded executor for handling operations on remotes.
     * These executors are typically called as part of Paxos verification from autobatched contexts, *but* the
     * individual executions are <b>not</b> autobatched. This means that if one node is performing slowly, calls
     * pending on that node may build up over time, eventually leading to thread explosion or OOMs. We thus limit
     * the size to {@link TimeLockPaxosExecutors#MAXIMUM_POOL_SIZE}.
     *
     * Users of such an executor should be prepared to handle {@link java.util.concurrent.RejectedExecutionException}.
     */
    static CheckedRejectionExecutorService createBoundedExecutor(int poolSize, String useCase, int index) {
        // metricRegistry is ignored because TExecutors.newCachedThreadPoolWithMaxThreads provides instrumentation.
        ExecutorService underlying =
                PTExecutors.newCachedThreadPoolWithMaxThreads(poolSize, "timelock-executors-" + useCase + "-" + index);
        return new CheckedRejectionExecutorService(underlying);
    }
}
