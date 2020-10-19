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

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.math.Fraction;

public final class Constants {
    private Constants() {
        // no op
    }

    public static final Duration HEALTH_FEEDBACK_REPORT_EXPIRATION_MINUTES = Duration.ofMinutes(2);
    public static final Fraction UNHEALTHY_CLIENTS_PROPORTION_LIMIT = Fraction.ONE_THIRD;

    public static final ServiceLevelObjectiveSpecification LEADER_TIME_SERVICE_LEVEL_OBJECTIVES =
            ServiceLevelObjectiveSpecification.builder()
                    .maximumPermittedSteadyStateP99(Duration.ofMillis(200))
                    .minimumRequestRateForConsideration(0.025)
                    .name("leaderTime")
                    .maximumPermittedErrorProportion(0.5)
                    .maximumPermittedQuietP99(Duration.ofSeconds(40))
                    .build();

    public static final ServiceLevelObjectiveSpecification START_TRANSACTION_SERVICE_LEVEL_OBJECTIVES =
            ServiceLevelObjectiveSpecification.builder()
                    .maximumPermittedSteadyStateP99(Duration.ofMillis(500))
                    .minimumRequestRateForConsideration(0.02)
                    .name("startTransaction")
                    .maximumPermittedErrorProportion(0.5)
                    .maximumPermittedQuietP99(Duration.ofSeconds(50))
                    .build();

    public static final int MIN_UNHEALTHY_SERVICES = 2;

    public static final Set<String> ATLAS_BLACKLISTED_VERSIONS = new HashSet<>();
}
