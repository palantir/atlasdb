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

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

final class TimeLockPaxosExecutors {
    @VisibleForTesting
    static final int MAXIMUM_POOL_SIZE = 100;

    private static final Duration THREAD_KEEP_ALIVE = Duration.ofSeconds(5);
    private static final int SINGLE_THREAD_FOR_MOSTLY_AUTOBATCHED_OPERATIONS = 1;

    private TimeLockPaxosExecutors() {
        // no
    }

    /**
     * Creates a mapping of services to {@link ExecutorService}s indicating that tasks oriented towards the relevant
     * node should be run on the associated executor.
     *
     * It is assumed that tasks run on the local node will return quickly (hence the use of the direct executor).
     */
    static <T> Map<T, ExecutorService> createBoundedExecutors(
            MetricRegistry metricRegistry, LocalAndRemotes<T> localAndRemotes, String useCase) {
        int numRemotes = localAndRemotes.remotes().size();
        ImmutableMap.Builder<T, ExecutorService> remoteExecutors = ImmutableMap.builderWithExpectedSize(numRemotes);
        for (int index = 0; index < numRemotes; index++) {
            T remote = localAndRemotes.remotes().get(index);
            remoteExecutors.put(remote, createBoundedExecutor(metricRegistry, useCase, index));
        }
        remoteExecutors.put(localAndRemotes.local(), MoreExecutors.newDirectExecutorService());
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
    static ExecutorService createBoundedExecutor(MetricRegistry metricRegistry, String useCase, int index) {
        return new InstrumentedExecutorService(
                PTExecutors.newThreadPoolExecutor(
                        SINGLE_THREAD_FOR_MOSTLY_AUTOBATCHED_OPERATIONS,
                        MAXIMUM_POOL_SIZE,
                        THREAD_KEEP_ALIVE.toMillis(),
                        TimeUnit.MILLISECONDS,
                        new SynchronousQueue<>(),
                        new NamedThreadFactory("timelock-executors-" + useCase, true)),
                metricRegistry,
                MetricRegistry.name(TimeLockPaxosExecutors.class, useCase, "executor-" + index));
    }
}
