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
package com.palantir.atlasdb.timelock.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;

@JsonSerialize(as = ImmutableTimeLimiterConfiguration.class)
@JsonDeserialize(as = ImmutableTimeLimiterConfiguration.class)
@Value.Immutable
public abstract class TimeLimiterConfiguration {
    public static final double DEFAULT_BLOCKING_TIMEOUT_ERROR_MARGIN = 0.03;

    /**
     * Returns true if and only if we want the time limiter to interrupt long-running requests before Jetty would
     * close the connection.
     */
    @Value.Parameter
    public abstract boolean enableTimeLimiting();

    /**
     * Returns a value indicating the margin of error we leave before interrupting a long running request,
     * since we wish to perform this interruption and return a BlockingTimeoutException _before_ Jetty closes the
     * stream. This margin is specified as a ratio of the smallest idle timeout - hence it must be in (0, 1).
     */
    @Value.Default
    @Value.Parameter
    public double blockingTimeoutErrorMargin() {
        return DEFAULT_BLOCKING_TIMEOUT_ERROR_MARGIN;
    }

    @Value.Check
    protected void check() {
        if (enableTimeLimiting()) {
            double errorMargin = blockingTimeoutErrorMargin();
            Preconditions.checkState(
                    errorMargin > 0 && errorMargin < 1,
                    "Lock service timeout margin must be strictly between 0 and 1 but found %s",
                    errorMargin);
        }
    }

    /**
     * Default configuration does not engage the time limiter at all.
     */
    public static TimeLimiterConfiguration getDefaultConfiguration() {
        return ImmutableTimeLimiterConfiguration.builder()
                .enableTimeLimiting(false)
                .build();
    }
}
