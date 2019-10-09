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

package com.palantir.common.proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

import org.junit.Before;
import org.junit.Test;

public class ExperimentRunningProxyTest {
    private static final int EXPERIMENTAL_INT = 123;
    private static final int FALLBACK_INT = 1;
    private static final RuntimeException RUNTIME_EXCEPTION = new RuntimeException("foo");

    private final IntSupplier experimentalIntSupplier = mock(IntSupplier.class);
    private final IntSupplier fallbackIntSupplier = mock(IntSupplier.class);
    private final BooleanSupplier useExperimental = mock(BooleanSupplier.class);
    private final Clock clock = mock(Clock.class);
    private final ExperimentRunningProxy<IntSupplier> proxy = new ExperimentRunningProxy<>(
            experimentalIntSupplier, fallbackIntSupplier, useExperimental, clock);

    private final IntSupplier experimentSupplier = intSupplierForProxy(proxy);

    @Before
    public void setup() {
        when(fallbackIntSupplier.getAsInt()).thenReturn(FALLBACK_INT);
        when(experimentalIntSupplier.getAsInt()).thenReturn(EXPERIMENTAL_INT);
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(0));
    }

    @Test
    public void doesNotAttemptExperimentIfNotRequested() {
        disableExperiment();
        assertThat(experimentSupplier.getAsInt()).isEqualTo(FALLBACK_INT);
    }

    @Test
    public void attemptsExperimentIfRequested() {
        enableExperiment();
        assertThat(experimentSupplier.getAsInt()).isEqualTo(EXPERIMENTAL_INT);
    }

    @Test
    public void canFallBackIfExperimentFails() {
        enableExperiment();
        throwOnFirstExperiment();

        assertThatThrownBy(experimentSupplier::getAsInt).isEqualTo(RUNTIME_EXCEPTION);
        assertThat(experimentSupplier.getAsInt()).isEqualTo(FALLBACK_INT);
    }

    @Test
    public void attemptsExperimentAgainAfterEnoughTimeHasElapsed() {
        enableExperiment();
        throwOnFirstExperiment();

        assertThatThrownBy(experimentSupplier::getAsInt).isEqualTo(RUNTIME_EXCEPTION);
        assertThat(experimentSupplier.getAsInt()).isEqualTo(FALLBACK_INT);

        when(clock.instant()).thenReturn(Instant.ofEpochSecond(1_000));

        assertThat(experimentSupplier.getAsInt()).isEqualTo(EXPERIMENTAL_INT);
    }

    @Test
    public void canLiveReloadUsageOfExperiment() {
        enableExperiment();
        assertThat(experimentSupplier.getAsInt()).isEqualTo(EXPERIMENTAL_INT);

        disableExperiment();
        assertThat(experimentSupplier.getAsInt()).isEqualTo(FALLBACK_INT);

        enableExperiment();
        assertThat(experimentSupplier.getAsInt()).isEqualTo(EXPERIMENTAL_INT);
    }

    private static IntSupplier intSupplierForProxy(ExperimentRunningProxy<IntSupplier> proxy) {
        return (IntSupplier) Proxy.newProxyInstance(IntSupplier.class.getClassLoader(),
                new Class[] { IntSupplier.class },
                proxy);
    }

    private void enableExperiment() {
        when(useExperimental.getAsBoolean()).thenReturn(true);
    }

    private void disableExperiment() {
        when(useExperimental.getAsBoolean()).thenReturn(false);
    }

    private void throwOnFirstExperiment() {
        when(experimentalIntSupplier.getAsInt()).thenThrow(RUNTIME_EXCEPTION).thenReturn(EXPERIMENTAL_INT);
    }
}
