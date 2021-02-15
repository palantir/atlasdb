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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;

public class ExperimentRunningProxyTest {
    private static final int EXPERIMENTAL_RESULT = 123;
    private static final int FALLBACK_RESULT = 1;
    private static final RuntimeException RUNTIME_EXCEPTION = new RuntimeException("foo");

    private final IntSupplier experimentalIntSupplier = mock(IntSupplier.class);
    private final Supplier<IntSupplier> experimentSupplier = mock(Supplier.class);
    private final IntSupplier fallbackIntSupplier = () -> FALLBACK_RESULT;
    private final BooleanSupplier useExperimental = mock(BooleanSupplier.class);
    private final BooleanSupplier enableFallback = mock(BooleanSupplier.class);
    private final Clock clock = mock(Clock.class);
    private final AtomicLong errorCounter = new AtomicLong();

    private final IntSupplier proxyInstance = intSupplierForProxy(new ExperimentRunningProxy<>(
            experimentSupplier,
            fallbackIntSupplier,
            useExperimental,
            enableFallback,
            clock,
            errorCounter::incrementAndGet));

    @Before
    public void setup() {
        when(experimentSupplier.get()).thenReturn(experimentalIntSupplier);
        returnValueOnExperiment();
        enableFallback();
        setTimeTo(Instant.ofEpochSecond(0));
    }

    @Test
    public void doesNotAttemptExperimentIfNotRequested() {
        disableExperiment();
        assertThat(proxyInstance.getAsInt()).isEqualTo(FALLBACK_RESULT);
        assertErrors(0L);
    }

    @Test
    public void attemptsExperimentIfRequested() {
        enableExperiment();
        assertThat(proxyInstance.getAsInt()).isEqualTo(EXPERIMENTAL_RESULT);
        assertErrors(0L);
    }

    @Test
    public void canFallBackIfExperimentFails() {
        enableExperiment();
        throwOnExperiment();

        assertThatThrownBy(proxyInstance::getAsInt).isEqualTo(RUNTIME_EXCEPTION);
        assertThat(proxyInstance.getAsInt()).isEqualTo(FALLBACK_RESULT);
        assertErrors(1L);
    }

    @Test
    public void doesNotFallBackWhenFallBackDisabled() {
        enableExperiment();
        disableFallback();
        throwOnExperiment();

        assertThatThrownBy(proxyInstance::getAsInt).isEqualTo(RUNTIME_EXCEPTION);
        assertThatThrownBy(proxyInstance::getAsInt).isEqualTo(RUNTIME_EXCEPTION);
        assertErrors(2L);
    }

    @Test
    public void attemptsExperimentAgainAfterEnoughTimeHasElapsed() {
        enableExperiment();

        setTimeTo(Instant.ofEpochSecond(0));
        throwOnExperiment();

        assertThatThrownBy(proxyInstance::getAsInt).isEqualTo(RUNTIME_EXCEPTION);
        assertThat(proxyInstance.getAsInt()).isEqualTo(FALLBACK_RESULT);
        returnValueOnExperiment();

        setTimeTo(Instant.ofEpochSecond(ExperimentRunningProxy.REFRESH_INTERVAL.getSeconds() - 1));
        assertThat(proxyInstance.getAsInt()).isEqualTo(FALLBACK_RESULT);

        setTimeTo(Instant.ofEpochSecond(ExperimentRunningProxy.REFRESH_INTERVAL.getSeconds() + 1));
        assertThat(proxyInstance.getAsInt()).isEqualTo(EXPERIMENTAL_RESULT);
        assertErrors(1L);
    }

    @Test
    public void canLiveReloadUsageOfExperiment() {
        enableExperiment();
        assertThat(proxyInstance.getAsInt()).isEqualTo(EXPERIMENTAL_RESULT);

        disableExperiment();
        assertThat(proxyInstance.getAsInt()).isEqualTo(FALLBACK_RESULT);

        enableExperiment();
        assertThat(proxyInstance.getAsInt()).isEqualTo(EXPERIMENTAL_RESULT);
        assertErrors(0L);
    }

    @Test
    public void canLiveReloadUsingFallBack() {
        enableExperiment();
        disableFallback();
        throwOnExperiment();

        assertThatThrownBy(proxyInstance::getAsInt).isEqualTo(RUNTIME_EXCEPTION);

        enableFallback();
        assertThatThrownBy(proxyInstance::getAsInt).isEqualTo(RUNTIME_EXCEPTION);
        assertThat(proxyInstance.getAsInt()).isEqualTo(FALLBACK_RESULT);

        disableFallback();
        assertThatThrownBy(proxyInstance::getAsInt).isEqualTo(RUNTIME_EXCEPTION);
        assertErrors(3L);
    }

    private static IntSupplier intSupplierForProxy(ExperimentRunningProxy<IntSupplier> proxy) {
        return (IntSupplier)
                Proxy.newProxyInstance(IntSupplier.class.getClassLoader(), new Class[] {IntSupplier.class}, proxy);
    }

    private void returnValueOnExperiment() {
        doReturn(EXPERIMENTAL_RESULT).when(experimentalIntSupplier).getAsInt();
    }

    private void throwOnExperiment() {
        when(experimentalIntSupplier.getAsInt()).thenThrow(RUNTIME_EXCEPTION);
    }

    private void setTimeTo(Instant instant) {
        when(clock.instant()).thenReturn(instant);
    }

    private void enableExperiment() {
        when(useExperimental.getAsBoolean()).thenReturn(true);
    }

    private void disableExperiment() {
        when(useExperimental.getAsBoolean()).thenReturn(false);
    }

    private void enableFallback() {
        when(enableFallback.getAsBoolean()).thenReturn(true);
    }

    private void disableFallback() {
        when(enableFallback.getAsBoolean()).thenReturn(false);
    }

    private void assertErrors(long number) {
        assertThat(errorCounter.get()).isEqualTo(number);
    }
}
