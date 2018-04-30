/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class TimeLockRuntimeConfigurationTest {
    @Test
    public void canCreateWithZeroClients() {
        ImmutableTimeLockRuntimeConfiguration.builder().build();
    }

    @Test
    public void canSpecifyPositiveLockLoggerTimeout() {
        ImmutableTimeLockRuntimeConfiguration.builder()
                .slowLockLogTriggerMillis(1L)
                .build();
    }

    @Test
    public void throwOnNegativeLeaderPingResponseWait() {
        assertThatThrownBy(() -> ImmutableTimeLockRuntimeConfiguration.builder()
                .slowLockLogTriggerMillis(-1L)
                .build()).isInstanceOf(IllegalStateException.class);
    }

}
