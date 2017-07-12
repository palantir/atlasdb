/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;

// TODO: Should we use this class instead of the Optional<Double> in TimeLockRuntimeConfiguration?
@JsonDeserialize(as = ImmutableTimeLimiterConfiguration.class)
@JsonSerialize(as = ImmutableTimeLimiterConfiguration.class)
@Value.Immutable
public interface TimeLimiterConfiguration {
    /**
     * Returns a value indicating the margin of error we leave before interrupting a long running request,
     * since we wish to perform this interruption and return a BlockingTimeoutException _before_ Jetty closes the
     * stream. This margin is specified as a ratio of the smallest idle timeout - hence it must be in (0, 1).
     */
    @JsonProperty("blocking-timeout-error-margin")
    @Value.Default
    default double blockingTimeoutErrorMargin() {
        return 0.03;
    }

    @Value.Check
    default void check() {
        Preconditions.checkState(
                blockingTimeoutErrorMargin() > 0 && blockingTimeoutErrorMargin() < 1,
                "Lock service timeout margin must be strictly between 0 and 1 but found %s",
                blockingTimeoutErrorMargin());
    }
}
