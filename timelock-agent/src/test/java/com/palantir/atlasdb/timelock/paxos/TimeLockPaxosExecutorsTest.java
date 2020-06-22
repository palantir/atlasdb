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
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.PTExecutors;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class TimeLockPaxosExecutorsTest {
    private static final String TEST = "test";
    private static final Callable<Integer> SLEEP_FOR_ONE_SECOND = () -> {
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        return 42;
    };

    private final Object local = new Object();
    private final Object remote1 = new Object();
    private final Object remote2 = new Object();
    private final List<Object> remotes = ImmutableList.of(remote1, remote2);

    private final LocalAndRemotes<Object> localAndRemotes = LocalAndRemotes.of(local, remotes);

    private final Map<Object, ExecutorService> executors = TimeLockPaxosExecutors.createBoundedExecutors(
            MetricsManagers.createForTests().getRegistry(),
            localAndRemotes,
            TEST);

    @Test
    public void hasKeysCollectivelyMatchingLocalAndRemoteElements() {
        assertThat(executors.keySet()).hasSameElementsAs(localAndRemotes.all());
    }

    @Test
    public void remoteExecutorsAreBounded() {
        for (int i = 0; i < TimeLockPaxosExecutors.MAXIMUM_POOL_SIZE; i++) {
            executors.get(remote1).submit(SLEEP_FOR_ONE_SECOND);
        }
        assertThatThrownBy(() -> executors.get(remote1).submit(SLEEP_FOR_ONE_SECOND))
                .isInstanceOf(RejectedExecutionException.class);
    }

    @Test
    public void remoteExecutorsAreLimitedSeparately() {
        for (int i = 0; i < TimeLockPaxosExecutors.MAXIMUM_POOL_SIZE; i++) {
            executors.get(remote1).submit(SLEEP_FOR_ONE_SECOND);
        }
        assertThatCode(() -> executors.get(remote2).submit(SLEEP_FOR_ONE_SECOND))
                .doesNotThrowAnyException();
    }
    @Test
    public void localExecutorsAreNotBoundedByMaximumPoolSize() {
        int numThreads = TimeLockPaxosExecutors.MAXIMUM_POOL_SIZE * 2;
        ExecutorService executor = PTExecutors.newFixedThreadPool(numThreads);
        List<Future<Integer>> results = IntStream.range(0, numThreads)
                .mapToObj(ignore -> executor.submit(this::submitToLocalAndGetUnchecked))
                .collect(Collectors.toList());
        results.forEach(future -> assertThatCode(() -> AtlasFutures.getUnchecked(future))
                .doesNotThrowAnyException());
    }

    private Integer submitToLocalAndGetUnchecked() {
        return AtlasFutures.getUnchecked(executors.get(local).submit(SLEEP_FOR_ONE_SECOND));
    }
}
