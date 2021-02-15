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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

@SuppressWarnings("CheckReturnValue")
public class PaxosRuntimeConfigurationTest {
    private static final long POSITIVE_LONG = 1L;
    private static final long NEGATIVE_LONG = -1L;

    @Test
    public void canSpecifyPositivePingRate() {
        ImmutablePaxosRuntimeConfiguration.builder().pingRateMs(POSITIVE_LONG).build();
    }

    @Test
    public void throwOnNegativePingRate() {
        assertThatThrownBy(ImmutablePaxosRuntimeConfiguration.builder().pingRateMs(NEGATIVE_LONG)::build)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void canSpecifyPositiveMaximumWaitBeforeProposingLeadership() {
        ImmutablePaxosRuntimeConfiguration.builder()
                .maximumWaitBeforeProposalMs(POSITIVE_LONG)
                .build();
    }

    @Test
    public void throwOnNegativeMaximumWaitBeforeProposingLeadership() {
        assertThatThrownBy(
                        ImmutablePaxosRuntimeConfiguration.builder().maximumWaitBeforeProposalMs(NEGATIVE_LONG)::build)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void canSpecifyPositiveLeaderPingResponseWait() {
        ImmutablePaxosRuntimeConfiguration.builder()
                .leaderPingResponseWaitMs(POSITIVE_LONG)
                .build();
    }

    @Test
    public void throwOnNegativeLeaderPingResponseWait() {
        assertThatThrownBy(ImmutablePaxosRuntimeConfiguration.builder().leaderPingResponseWaitMs(NEGATIVE_LONG)::build)
                .isInstanceOf(IllegalArgumentException.class);
    }
}
