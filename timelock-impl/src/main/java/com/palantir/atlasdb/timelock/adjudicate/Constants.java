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
import java.util.Set;

import org.apache.commons.lang3.math.Fraction;

import com.google.common.collect.Sets;

public final class Constants {
    private Constants() {
        // no op
    }

    public static final int HEALTH_FEEDBACK_REPORT_EXPIRATION_MINUTES = 2;
    public static final Fraction UNHEALTHY_CLIENTS_PROPORTION_LIMIT = Fraction.ONE_THIRD;
    public static final int MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE = 60;
    public static final Duration MAX_ACCEPTABLE_LEADER_TIME_P99_MILLI = Duration.ofMillis(200);
    public static final double LEADER_TIME_ERROR_RATE_THRESHOLD = 30;

    public static final int MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE = 60;
    public static final Duration MAX_ACCEPTABLE_START_TXN_P99_MILLI = Duration.ofMillis(500);
    public static final double START_TXN_ERROR_RATE_THRESHOLD = 30;
    public static final int MIN_UNHEALTHY_SERVICES = 2;

    public static final Set<String> ATLAS_BLACKLISTED_VERSIONS = Sets.newHashSet();
}