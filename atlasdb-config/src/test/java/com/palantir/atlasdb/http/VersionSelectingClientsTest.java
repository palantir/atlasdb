/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.junit.Test;

import com.google.common.util.concurrent.AtomicDouble;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.http.VersionSelectingClients.VersionSelectingConfig;
import com.palantir.atlasdb.util.AccumulatingValueMetric;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.proxy.ExperimentRunningProxy;
import com.palantir.timestamp.TimestampService;

public class VersionSelectingClientsTest {
    private final AtomicDouble probability = new AtomicDouble(0.0);
    private final AtomicBoolean fallback = new AtomicBoolean(true);
    private final VersionSelectingConfig config = VersionSelectingConfig
            .withNewClientProbability(probability::get, fallback::get);
    private final TimestampService newService = mock(TimestampService.class);
    private final TimestampService oldService = mock(TimestampService.class);
    private final MetricsManager metricsManager = MetricsManagers.createForTests();

    private final TimestampService selectingService = VersionSelectingClients.createVersionSelectingClient(
            metricsManager,
            ImmutableInstanceAndVersion.of(newService, "new"),
            ImmutableInstanceAndVersion.of(oldService, "old"),
            config,
            TimestampService.class);

    @Test
    public void probabilityZeroAlwaysGoesToOldService() {
        probability.set(0.0);
        IntStream.range(0, 100)
                .forEach($ -> selectingService.getFreshTimestamp());
        verify(oldService, times(100)).getFreshTimestamp();
    }

    @Test
    public void respondsToChangesInProbabilityAssignment() {
        selectingService.getFreshTimestamps(1);
        verify(oldService).getFreshTimestamps(1);
        verify(newService, never()).getFreshTimestamps(1);

        probability.set(1.0);
        selectingService.getFreshTimestamps(2);
        verify(oldService, never()).getFreshTimestamps(2);
        verify(newService).getFreshTimestamps(2);
    }

    @Test
    public void drawsSeparateSamples() {
        probability.set(0.5);
        IntStream.range(0, 100)
                .forEach($ -> selectingService.getFreshTimestamp());
        verify(oldService, atLeast(10)).getFreshTimestamp();
        verify(newService, atLeast(10)).getFreshTimestamp();
    }

    @Test
    public void supportsLiveReloadableFallback() {
        probability.set(1.0);
        fallback.set(true);
        setNewServiceToThrow();

        assertThatThrownBy(selectingService::getFreshTimestamp).isInstanceOf(RuntimeException.class);
        selectingService.getFreshTimestamp();

        verify(newService).getFreshTimestamp();
        verify(oldService).getFreshTimestamp();

        fallback.set(false);
        assertThatThrownBy(selectingService::getFreshTimestamp).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(selectingService::getFreshTimestamp).isInstanceOf(RuntimeException.class);

        verify(newService, times(3)).getFreshTimestamp();
        verify(oldService).getFreshTimestamp();
    }

    @Test
    public void allServicesUseTheSameErrorMetric() {
        probability.set(1.0);
        setNewServiceToThrow();

        Runnable oldTask = mock(Runnable.class);
        Runnable newTask = mock(Runnable.class);

        doThrow(new RuntimeException()).when(oldTask).run();

        Runnable selectingTask = VersionSelectingClients.createVersionSelectingClient(
                metricsManager,
                ImmutableInstanceAndVersion.of(oldTask, "old"),
                ImmutableInstanceAndVersion.of(newTask, "new"),
                VersionSelectingConfig.withNewClientProbability(() -> 1.0, () -> true),
                Runnable.class);

        assertThatThrownBy(selectingService::getFreshTimestamp).isInstanceOf(RuntimeException.class);
        selectingService.getFreshTimestamp();
        assertErrorMetricEquals(1L);

        assertThatThrownBy(selectingTask::run).isInstanceOf(RuntimeException.class);
        selectingTask.run();
        assertErrorMetricEquals(2L);
    }

    private void setNewServiceToThrow() {
        when(newService.getFreshTimestamp()).thenThrow(new RuntimeException());
    }

    private void assertErrorMetricEquals(long errors) {
        AccumulatingValueMetric gauge = metricsManager.registerOrGetGauge(
                ExperimentRunningProxy.class,
                AtlasDbMetricNames.EXPERIMENT_ERRORS,
                AccumulatingValueMetric::new);
        assertThat(gauge.getValue()).isEqualTo(errors);
    }
}
