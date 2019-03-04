/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class TimeLimiterConfigurationTest {
    @Test
    public void defaultConfigurationHasTimeLimitingDisabled() {
        TimeLimiterConfiguration defaultConfiguration = TimeLimiterConfiguration.getDefaultConfiguration();
        assertThat(defaultConfiguration.enableTimeLimiting()).isFalse();
    }

    @Test
    public void canCreateConfigurationWithBlockingTimeoutErrorMarginInBounds() {
        assertIsValidErrorMargin(0.08);
        assertIsValidErrorMargin(0.9999);
    }

    @Test
    public void createsConfigurationWithDefaultBlockingTimeoutErrorMarginIfNotSpecified() {
        TimeLimiterConfiguration configuration = ImmutableTimeLimiterConfiguration.builder()
                .enableTimeLimiting(true)
                .build();
        assertThat(configuration.enableTimeLimiting()).isTrue();
        assertThat(configuration.blockingTimeoutErrorMargin())
                .isEqualTo(TimeLimiterConfiguration.DEFAULT_BLOCKING_TIMEOUT_ERROR_MARGIN);
    }

    @Test
    public void throwsIfBlockingTimeoutErrorMarginIsAtLeastOne() {
        assertIsNotValidErrorMargin(1.0);
        assertIsNotValidErrorMargin(Math.PI);
    }

    @Test
    public void throwsIfBlockingTimeoutErrorMarginIsNonpositive() {
        assertIsNotValidErrorMargin(0.0);
        assertIsNotValidErrorMargin(-3.21);
    }

    @Test
    public void ignoresErrorMarginIfTimeLimitingIsDisabled() {
        TimeLimiterConfiguration configuration = ImmutableTimeLimiterConfiguration.of(false, Math.PI);
        assertThat(configuration.enableTimeLimiting()).isFalse();
    }

    private static void assertIsValidErrorMargin(double errorMargin) {
        assertThat(createConfigurationWithErrorMargin(errorMargin).blockingTimeoutErrorMargin())
                .isEqualTo(errorMargin);
    }

    private static void assertIsNotValidErrorMargin(double errorMargin) {
        assertThatThrownBy(() -> createConfigurationWithErrorMargin(errorMargin))
                .isInstanceOf(IllegalStateException.class);
    }

    private static TimeLimiterConfiguration createConfigurationWithErrorMargin(double errorMargin) {
        return ImmutableTimeLimiterConfiguration.of(true, errorMargin);
    }
}
