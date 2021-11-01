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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.common.concurrent.CheckedRejectedExecutionException;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.concurrent.PTExecutors;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class TimeLockPaxosExecutorsTest {
    private static final String TEST = "test";
    private static final Callable<Integer> BLOCKING_TASK = () -> {
        Uninterruptibles.sleepUninterruptibly(Duration.ofNanos(Long.MAX_VALUE));
        return 42;
    };

    private final Object local = new Object();
    private final Object remote1 = new Object();
    private final Object remote2 = new Object();
    private final List<Object> remotes = ImmutableList.of(remote1, remote2);

    private final LocalAndRemotes<Object> localAndRemotes = LocalAndRemotes.of(local, remotes);

    private final Map<Object, CheckedRejectionExecutorService> executors =
            TimeLockPaxosExecutors.createBoundedExecutors(
                    TimeLockPaxosExecutors.MAXIMUM_POOL_SIZE, localAndRemotes, TEST);

    @Test
    public void hasKeysCollectivelyMatchingLocalAndRemoteElements() {
        assertThat(executors.keySet()).hasSameElementsAs(localAndRemotes.all());
    }

    @Test
    public void remoteExecutorsAreBounded() throws CheckedRejectedExecutionException {
        for (int i = 0; i < TimeLockPaxosExecutors.MAXIMUM_POOL_SIZE; i++) {
            executors.get(remote1).submit(BLOCKING_TASK);
        }
        assertThatThrownBy(() -> executors.get(remote1).submit(BLOCKING_TASK))
                .isInstanceOf(CheckedRejectedExecutionException.class)
                .hasCauseInstanceOf(RejectedExecutionException.class);
    }

    @Test
    public void remoteExecutorsAreLimitedSeparately() throws CheckedRejectedExecutionException {
        for (int i = 0; i < TimeLockPaxosExecutors.MAXIMUM_POOL_SIZE; i++) {
            executors.get(remote1).submit(BLOCKING_TASK);
        }
        assertThatCode(() -> executors.get(remote2).submit(BLOCKING_TASK)).doesNotThrowAnyException();
    }

    @Test
    public void localExecutorsAreNotBoundedByMaximumPoolSize() {
        int numThreads = TimeLockPaxosExecutors.MAXIMUM_POOL_SIZE * 2;
        ExecutorService executor = PTExecutors.newFixedThreadPool(numThreads);
        List<Future<Integer>> results = IntStream.range(0, numThreads)
                .mapToObj(_ignore -> executor.submit(this::submitToLocalAndGetUnchecked))
                .collect(Collectors.toList());
        results.forEach(future ->
                assertThatCode(() -> AtlasFutures.getUnchecked(future)).doesNotThrowAnyException());
    }

    private Integer submitToLocalAndGetUnchecked() throws CheckedRejectedExecutionException {
        return AtlasFutures.getUnchecked(executors.get(local).submit(() -> 1));
    }
}
