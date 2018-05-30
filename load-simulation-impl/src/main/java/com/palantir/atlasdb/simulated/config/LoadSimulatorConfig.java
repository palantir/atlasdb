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

package com.palantir.atlasdb.simulated.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;

@Value.Immutable
@JsonDeserialize(as = ImmutableLoadSimulatorConfig.class)
@JsonSerialize(as = ImmutableLoadSimulatorConfig.class)
public interface LoadSimulatorConfig {
    LoadSimulatorConfig DEFAULT = LoadSimulatorConfig.builder().build();

    @Value.Default
    default boolean enabled() {
        return false;
    }

    @Value.Default
    default int executorThreads() {
        return 1;
    }

    @Value.Check
    default void check() {
        if (!enabled()) {
            return;
        }
        Preconditions.checkState(executorThreads() > 0);
    }

    static ImmutableLoadSimulatorConfig.Builder builder() {
        return ImmutableLoadSimulatorConfig.builder();
    }
}
