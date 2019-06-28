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

package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.palantir.timelock.config.TargetedSweepLockControlConfig.Mode;

public class TargetedSweepLockControlConfigTests {

    @Test
    public void testWhitelist() {
        TargetedSweepLockControlConfig config = config(Mode.DISABLED_BY_DEFAULT, "should-rate-limit");
        assertThat(config.shouldRateLimit("a random client"))
                .as("rate limiting is disabled by default for all clients other than the exclusions")
                .isFalse();

        assertThat(config.shouldRateLimit("should-rate-limit"))
                .as("rate limiting is disabled by default for all clients, except for 'should-rate-limit'")
                .isTrue();
    }

    @Test
    public void testBlacklist() {
        TargetedSweepLockControlConfig config = config(Mode.ENABLED_BY_DEFAULT, "should-not-rate-limit");
        assertThat(config.shouldRateLimit("a random client"))
                .as("rate limiting is enabled by default for all clients other than the exclusions")
                .isTrue();

        assertThat(config.shouldRateLimit("should-not-rate-limit"))
                .as("rate limiting is enabled by default for all clients, except for 'should-not-rate-limit', where "
                        + "it should process as many requests as possible as it receives")
                .isFalse();
    }

    private static TargetedSweepLockControlConfig config(Mode mode, String... exclusions) {
        return ImmutableTargetedSweepLockControlConfig.builder()
                .mode(mode)
                .addExcludedClients(exclusions)
                .build();
    }
}
