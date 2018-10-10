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
package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class TimeLockDeprecatedConfigurationTest {
    private static final int POSITIVE_INT = 1;
    private static final int NEGATIVE_INT = -1;

    private static final long POSITIVE_LONG = 1L;
    private static final long NEGATIVE_LONG = -1L;

    @Test
    public void canSpecifyPositiveNumberOfAvailableThreads() {
        ImmutableTimeLockDeprecatedConfiguration.builder()
                .availableThreads(POSITIVE_INT)
                .build();
    }

    @Test
    public void throwsOnNegativeNumberOfAvailableThreads() {
        assertThatThrownBy(ImmutableTimeLockDeprecatedConfiguration.builder()
                .availableThreads(NEGATIVE_INT)
                ::build).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void canSpecifyPositiveBlockingTimeout() {
        ImmutableTimeLockDeprecatedConfiguration.builder()
                .blockingTimeoutInMs(POSITIVE_LONG)
                .build();
    }

    @Test
    public void throwsOnNegativeBlockingTimeout() {
        assertThatThrownBy(ImmutableTimeLockDeprecatedConfiguration.builder()
                .blockingTimeoutInMs(NEGATIVE_LONG)
                ::build).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void doesNotEnableThreadRequestLimitByDefault() {
        assertThat(ImmutableTimeLockDeprecatedConfiguration.builder().build().useClientRequestLimit()).isFalse();
    }

    @Test
    public void doesNotEnableTimeLimiterByDefault() {
        assertThat(ImmutableTimeLockDeprecatedConfiguration.builder().build().useLockTimeLimiter()).isFalse();
    }

}
