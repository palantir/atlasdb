/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.adjudicate;

import com.palantir.logsafe.Preconditions;
import java.time.Duration;
import org.immutables.value.Value;

@Value.Immutable
public interface ServiceLevelObjectiveSpecification {
    Duration maximumPermittedSteadyStateP99();

    // This is to catch badness when a request/ auto-batched requests take too long,
    // dropping the request rate below min threshold
    Duration maximumPermittedQuietP99();

    String name();

    double maximumPermittedErrorProportion();

    double minimumRequestRateForConsideration();

    @Value.Check
    default void check() {
        Preconditions.checkState(
                !maximumPermittedSteadyStateP99().isNegative(), "Cannot declare negative p99 service level objective");
        Preconditions.checkState(
                maximumPermittedErrorProportion() >= 0 && maximumPermittedErrorProportion() <= 1,
                "Permitted error proportion must be between 0 and 1.");
        Preconditions.checkState(minimumRequestRateForConsideration() >= 0, "Cannot declare negative min request rate");
        Preconditions.checkState(
                maximumPermittedQuietP99().toNanos()
                        > maximumPermittedSteadyStateP99().toNanos(),
                "p99 limit when request rate is low must be greater than the p99 limit in steady state.");
    }

    static ImmutableServiceLevelObjectiveSpecification.Builder builder() {
        return ImmutableServiceLevelObjectiveSpecification.builder();
    }
}
