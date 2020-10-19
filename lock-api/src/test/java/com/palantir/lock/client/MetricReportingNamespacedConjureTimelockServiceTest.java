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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class MetricReportingNamespacedConjureTimelockServiceTest {

    private static final UUID LEADER_1 = UUID.randomUUID();
    private ConjureStartTransactionsRequest startTransactionsRequest =
            mock(ConjureStartTransactionsRequest.class);
    private ConjureStartTransactionsResponse startTransactionsResponse =
            mock(ConjureStartTransactionsResponse.class);
    private LockWatchStateUpdate stateUpdate =
            LockWatchStateUpdate.success(LEADER_1, -1L, ImmutableList.of());
    private NamespacedConjureTimelockService delegate = mock(NamespacedConjureTimelockService.class);
    private TaggedMetricRegistry metricRegistry = mock(TaggedMetricRegistry.class);
    private Timer mockedTimer = mock(Timer.class);

    private NamespacedConjureTimelockService timelockService;

    @Before
    public void before() {
        timelockService = new MetricReportingNamespacedConjureTimelockService(delegate, metricRegistry);
        when(metricRegistry.timer(any())).thenReturn(mockedTimer);
    }

    @Test
    public void firstCallDoesNotReportMetrics() {
        when(delegate.startTransactions(any())).thenReturn(startTransactionsResponse);
        when(startTransactionsResponse.getLockWatchUpdate()).thenReturn(stateUpdate);
        timelockService.startTransactions(startTransactionsRequest);
        verifyNoInteractions(metricRegistry);
    }

    @Test
    public void sameLeaderDoesNotReportMetrics() {
        when(delegate.startTransactions(any())).thenReturn(startTransactionsResponse);
        when(startTransactionsResponse.getLockWatchUpdate()).thenReturn(stateUpdate);
        timelockService.startTransactions(startTransactionsRequest);
        timelockService.startTransactions(startTransactionsRequest);
        verifyNoInteractions(metricRegistry);
    }

    @Test
    public void differentLeaderReportsMetrics() {
        when(delegate.startTransactions(any())).thenReturn(startTransactionsResponse);
        when(startTransactionsResponse.getLockWatchUpdate())
                .thenReturn(stateUpdate)
                .thenReturn(LockWatchStateUpdate.snapshot(UUID.randomUUID(), 1L, ImmutableSet.of(), ImmutableSet.of()));
        timelockService.startTransactions(startTransactionsRequest);
        timelockService.startTransactions(startTransactionsRequest);
        verify(metricRegistry).timer(any());
        verify(mockedTimer).update(anyLong(), eq(TimeUnit.NANOSECONDS));
    }
}