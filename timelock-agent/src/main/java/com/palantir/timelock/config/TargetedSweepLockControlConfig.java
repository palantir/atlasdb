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

import java.util.Set;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonDeserialize(as = ImmutableTargetedSweepLockControlConfig.class)
@JsonSerialize(as = ImmutableTargetedSweepLockControlConfig.class)
public abstract class TargetedSweepLockControlConfig {

    public enum Mode {
        ENABLED_BY_DEFAULT,
        DISABLED_BY_DEFAULT
    }

    @Value.Default
    Mode mode() {
        return Mode.DISABLED_BY_DEFAULT;
    }

    @JsonProperty("excluded-clients")
    abstract Set<String> excludedClients();

    public boolean shouldRateLimit(String client) {
        return excludedClients().contains(client) ^ mode() == Mode.ENABLED_BY_DEFAULT;
    }

    public static TargetedSweepLockControlConfig defaultConfig() {
        return ImmutableTargetedSweepLockControlConfig.builder().build();
    }
}
