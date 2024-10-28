/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.batch;

import static org.assertj.core.api.Assertions.assertThatCode;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.AsyncTimelockServiceImpl;
import com.palantir.atlasdb.timelock.api.GetMinLeasedTimestampRequests;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.TimestampLeaseName;
import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.lockwatches.BufferMetrics;
import com.palantir.atlasdb.timelock.lockwatches.RequestMetrics;
import com.palantir.atlasdb.timelock.timestampleases.TimestampLeaseMetrics;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.List;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.Test;

public final class RemotingTimestampLeaseServiceAdapterTest {
    private final DeterministicScheduler executor = new DeterministicScheduler();

    @Test
    public void duplicateTimestampLeaseNameInGetMinLeasedTimestampsRequestDoesNotCauseException() {
        RemotingTimestampLeaseServiceAdapter adapter =
                new RemotingTimestampLeaseServiceAdapter((_unused1, _unused2) -> createTimelockService());
        Namespace namespace = Namespace.of("namespace");
        TimestampLeaseName timestampName = TimestampLeaseName.of("timestampName");
        assertThatCode(() -> unwrap(adapter.getMinLeasedTimestamps(
                        namespace, GetMinLeasedTimestampRequests.of(List.of(timestampName, timestampName)), null)))
                .doesNotThrowAnyException();
    }

    private <T> T unwrap(ListenableFuture<T> future) {
        executor.runUntilIdle();
        return AtlasFutures.getUnchecked(future);
    }

    private AsyncTimelockService createTimelockService() {
        AsyncLockService lockService = AsyncLockService.createDefault(
                new LockLog(new MetricRegistry(), () -> 100L),
                executor,
                executor,
                BufferMetrics.of(new DefaultTaggedMetricRegistry()),
                TimestampLeaseMetrics.of(new DefaultTaggedMetricRegistry()));

        return new AsyncTimelockServiceImpl(
                lockService,
                new InMemoryTimestampService(),
                new LockLog(new MetricRegistry(), () -> 1000L),
                RequestMetrics.of(new DefaultTaggedMetricRegistry()));
    }
}
