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

import java.util.List;

import com.google.common.collect.ImmutableList;

public final class Constants {
    private Constants() {
        // no op
    }

    public static final int HEALTH_FEEDBACK_REPORT_EXPIRATION_MINUTES = 2;
    public static final double UNHEALTHY_CLIENTS_PROPORTION_LIMIT = 0.33;
    public static final int MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE = 60;
    public static final int MAX_ACCEPTABLE_LEADER_TIME_P99_MILLI = 200;
    public static final int MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE = 1000; // todo Sudiksha
    public static final int MAX_ACCEPTABLE_START_TXN_P99_MILLI = 0;


    public static final List<String> ATLAS_BLACKLISTED_VERSIONS = ImmutableList.of();
}