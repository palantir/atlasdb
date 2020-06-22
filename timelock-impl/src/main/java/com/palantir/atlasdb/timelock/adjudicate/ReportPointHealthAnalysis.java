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

import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;
import com.palantir.timelock.feedback.EndpointStatistics;

public class ReportPointHealthAnalysis {

    static HealthStatus isHealthyHealthReport(ConjureTimeLockClientFeedback healthReport) {
//        if (AtlasDbVersionBlacklist.isBlacklistedVersion(healthReport.getAtlasVersion())) {
//            return HealthStatus.IGNORED;
//        }

        if (healthReport.getLeaderTime().isPresent()) {
            return getHealthStatus(healthReport.getLeaderTime().get());
        }

        if (healthReport.getStartTransaction().isPresent()) {
            return getHealthStatus(healthReport.getStartTransaction().get());
        }

        return HealthStatus.IGNORED;
    }

    static private HealthStatus getHealthStatus(EndpointStatistics endpointStatistics) {
//        if (endpointStatistics.getOneMin() < minRequiredLeaderTimeRate) {
//            return HealthStatus.IGNORED;
//        }
//        return endpointStatistics.getP99() > maxAcceptableLeaderTimeP99
//                ? HealthStatus.UNHEALTHY : HealthStatus.HEALTHY;

        return HealthStatus.IGNORED;

    }
}
