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

package com.palantir.atlasdb.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;

@JsonSerialize(as = ImmutableRemotingClientConfig.class)
@JsonDeserialize(as = ImmutableRemotingClientConfig.class)
@Value.Immutable
public interface RemotingClientConfig {
    RemotingClientConfig DEFAULT = ImmutableRemotingClientConfig.builder().build();
    RemotingClientConfig ALWAYS_USE_CONJURE = ImmutableRemotingClientConfig.builder()
            .maximumConjureRemotingProbability(1.0)
            .enableLegacyClientFallback(false)
            .build();

    @Value.Default
    default double maximumConjureRemotingProbability() {
        return 0.0;
    }

    @Value.Default
    default boolean enableLegacyClientFallback() {
        return true;
    }

    @Value.Check
    default void check() {
        Preconditions.checkState(0.0 <= maximumConjureRemotingProbability()
                        && maximumConjureRemotingProbability() <= 1.0,
                "Maximum probability of choosing v2 must be between 0.0 and 1.0 inclusive",
                SafeArg.of("probability", maximumConjureRemotingProbability()));
    }
}
