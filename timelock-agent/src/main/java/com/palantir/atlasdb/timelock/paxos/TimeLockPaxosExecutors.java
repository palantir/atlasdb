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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.streams.KeyedStream;

final class TimeLockPaxosExecutors {
    @VisibleForTesting
    static final int MAXIMUM_POOL_SIZE = 100;

    private static final Duration THREAD_KEEP_ALIVE = Duration.ofSeconds(5);

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
        Map<T, ExecutorService> remoteExecutors = KeyedStream.of(localAndRemotes.remotes())
                .map(remote -> createBoundedExecutor(metricRegistry, useCase))
                .collectToMap();
        remoteExecutors.put(localAndRemotes.local(), MoreExecutors.newDirectExecutorService());
        return remoteExecutors;
    }

    private static ExecutorService createBoundedExecutor(MetricRegistry metricRegistry, String useCase) {
        return new InstrumentedExecutorService(
                PTExecutors.newThreadPoolExecutor(
                        1, // Many operations are autobatched, so under ordinary circumstances 1 thread will do
                        MAXIMUM_POOL_SIZE, // Want to bound the number of threads that might be stuck
                        THREAD_KEEP_ALIVE.toMillis(),
                        TimeUnit.MILLISECONDS,
                        new SynchronousQueue<>(), // Prefer to avoid OOM risk if we get hammered
                        new ThreadFactoryBuilder()
                                .setNameFormat("timelock-executors-" + useCase + "-%d")
                                .setDaemon(true)
                                .build()),
                metricRegistry,
                MetricRegistry.name(TimeLockPaxosExecutors.class, useCase, "executor"));
    }
}
