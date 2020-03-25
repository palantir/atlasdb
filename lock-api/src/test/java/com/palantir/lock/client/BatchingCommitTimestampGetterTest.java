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

package com.palantir.lock.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.watch.ImmutableIdentifiedVersion;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchStateUpdate;

public class BatchingCommitTimestampGetterTest {
    private final NamespacedConjureTimelockService timelock = mock(NamespacedConjureTimelockService.class);
    private final LockWatchEventCache cache = mock(LockWatchEventCache.class);

    private BatchingCommitTimestampGetter getter;

    @Before
    public void setup() {
        AtomicLong counter = new AtomicLong(0);
        when(timelock.getCommitTimestamps(any())).thenAnswer(invocation -> {
            GetCommitTimestampsRequest request = invocation.getArgument(0, GetCommitTimestampsRequest.class);
            long lowerBound = counter.getAndAdd(request.getNumTimestamps());
            return GetCommitTimestampsResponse.builder()
                    .inclusiveLower(lowerBound)
                    .inclusiveUpper(lowerBound + request.getNumTimestamps() - 1)
                    .lockWatchUpdate(LockWatchStateUpdate.failed(UUID.randomUUID()))
                    .build();
        });
        when(cache.lastKnownVersion()).thenReturn(ImmutableIdentifiedVersion.of(UUID.randomUUID(), Optional.empty()));
        getter = BatchingCommitTimestampGetter.create(timelock, cache);
    }

    @After
    public void close() {
        getter.close();
    }

    @Test
    public void atLeastSomeRequestsGetBatched() {
        int poolSize = 1024;
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(
                PTExecutors.newFixedThreadPool(poolSize));

        List<Long> results = AtlasFutures.getUnchecked(Futures.allAsList(
                IntStream.range(0, poolSize)
                        .mapToObj(i -> executor.submit(getter::getCommitTimestamp))
                        .collect(Collectors.toList())));
        executor.shutdown();

        assertThat(results.stream().sorted().collect(Collectors.toList()))
                .isEqualTo(LongStream.range(0, poolSize).boxed().collect(Collectors.toList()));

        verify(timelock, atLeastOnce()).getCommitTimestamps(any());
        verify(cache, atLeastOnce()).processGetCommitTimestampsUpdate(anyCollection(), any(LockWatchStateUpdate.class));
        verify(timelock, atMost(poolSize - 1)).getCommitTimestamps(any());
    }
}
