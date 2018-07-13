/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TimeLockRuntimeConfigTest {

    private static final ServerListConfig serverListConfig = ImmutableServerListConfig.builder()
            .addServers("https://fooBarAddress")
            .build();
    private static final String illegalStateExceptionMessage = "If there is at least one entry in the server list,"
            + " isUsingTimelock field cannot be false.";

    @Test
    public void canCrateWithNoServers() {
        ImmutableTimeLockRuntimeConfig.builder().build();
    }

    @Test
    public void shouldNotUseTimelockWithDefaultValues() {
        TimeLockRuntimeConfig timeLockRuntimeConfig = ImmutableTimeLockRuntimeConfig.builder().build();
        assertFalse(timeLockRuntimeConfig.shouldUseTimelock());
    }

    @Test
    public void shouldUseTimelockWithNoServersAndIsUsingTimelockSet() {
        TimeLockRuntimeConfig timeLockRuntimeConfig = ImmutableTimeLockRuntimeConfig.builder()
                .isUsingTimelock(true)
                .build();
        assertTrue(timeLockRuntimeConfig.shouldUseTimelock());
    }

    @Test
    public void shouldUseTimelockWithAtLeastOneServerAndIsUsingTimelockSet() {
        TimeLockRuntimeConfig timeLockRuntimeConfig = ImmutableTimeLockRuntimeConfig.builder()
                .isUsingTimelock(true)
                .build();
        assertTrue(timeLockRuntimeConfig.shouldUseTimelock());
    }

    @Test
    public void throwsWithAtLeastOneServerAndDefaultValueForIsUsingTimelock() {
        assertThatThrownBy(() -> ImmutableTimeLockRuntimeConfig.builder()
                .serversList(serverListConfig)
                .build()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(illegalStateExceptionMessage);
    }

    @Test
    public void throwsWithAtLeastOneServerAndIsUsingTimelockSetToFalse() {
        assertThatThrownBy(() -> ImmutableTimeLockRuntimeConfig.builder()
                .serversList(serverListConfig)
                .isUsingTimelock(false)
                .build()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(illegalStateExceptionMessage);

    }

}
