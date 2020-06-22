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

package com.palantir.atlasdb.keyvalue.api.watch;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class ResilientLockWatchEventCacheTest {

    private final MetricsManager metricsManager = new MetricsManager(
            new MetricRegistry(),
            new DefaultTaggedMetricRegistry(),
            unused -> false);

    @Mock
    private LockWatchEventCache defaultCache;
    @Mock
    private LockWatchEventCache fallbackCache;
    private LockWatchEventCache proxyCache;

    @Before
    public void before() {
        proxyCache = ResilientLockWatchEventCache.newProxyInstance(defaultCache, fallbackCache, metricsManager);
    }

    @Test
    public void failCausesFallbackCacheToBeUsed() {
        RuntimeException runtimeException = new RuntimeException();
        when(defaultCache.getCommitUpdate(anyLong())).thenThrow(runtimeException);
        assertThatThrownBy(() -> proxyCache.getCommitUpdate(0L)).hasRootCause(runtimeException);

        // no op cache returns empty on last known version, so this should prove that we delegate there correctly
        proxyCache.lastKnownVersion();
        verify(fallbackCache).lastKnownVersion();
        verify(defaultCache, never()).lastKnownVersion();
    }
}
