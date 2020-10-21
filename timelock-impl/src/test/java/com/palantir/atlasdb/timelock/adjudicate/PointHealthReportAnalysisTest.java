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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.timelock.feedback.EndpointStatistics;
import org.junit.Test;

public class PointHealthReportAnalysisTest {
    private static final String CLIENT_1 = "Client1";
    private static final String LEADER_TIME = "leaderTime";

    private static final double RATE =
            Constants.LEADER_TIME_SERVICE_LEVEL_OBJECTIVES.minimumRequestRateForConsideration();
    private static final long P_99 = Constants.LEADER_TIME_SERVICE_LEVEL_OBJECTIVES
            .maximumPermittedSteadyStateP99()
            .toNanos();
    private static final double ERROR_PROPORTION =
            Constants.LEADER_TIME_SERVICE_LEVEL_OBJECTIVES.maximumPermittedErrorProportion();
    private static final long QUIET_P99 = Constants.LEADER_TIME_SERVICE_LEVEL_OBJECTIVES
            .maximumPermittedQuietP99()
            .toNanos();

    @Test
    public void reportIsHealthyWhenEverythingIsRight() {
        FeedbackHandler feedbackHandler = FeedbackHandler.createForTests();
        EndpointStatistics statistics = EndpointStatistics.builder()
                .p99(P_99 - 1)
                .oneMin(RATE + 1)
                .errorRate(0)
                .build();
        HealthStatus healthStatus = getHealthStatus(feedbackHandler, statistics);
        assertThat(healthStatus).isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void reportStatusIsUnknownIfReqRateIsBelowThreshold() {
        FeedbackHandler feedbackHandler = FeedbackHandler.createForTests();
        EndpointStatistics statistics = EndpointStatistics.builder()
                .p99(P_99 + 1)
                .oneMin(RATE - 0.01)
                .errorRate(ERROR_PROPORTION * RATE + 0.1)
                .build();
        HealthStatus healthStatus = getHealthStatus(feedbackHandler, statistics);
        assertThat(healthStatus).isEqualTo(HealthStatus.UNKNOWN);
    }

    @Test
    public void reportStatusIsUnhealthyIfErrProportionIsHigh() {
        FeedbackHandler feedbackHandler = FeedbackHandler.createForTests();
        EndpointStatistics statistics = EndpointStatistics.builder()
                .p99(P_99 + 1)
                .oneMin(RATE + 0.01)
                .errorRate(ERROR_PROPORTION * RATE + 0.1)
                .build();
        HealthStatus healthStatus = getHealthStatus(feedbackHandler, statistics);
        assertThat(healthStatus).isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    public void reportStatusIsUnhealthyIfP99IsAboveThreshold() {
        FeedbackHandler feedbackHandler = FeedbackHandler.createForTests();
        EndpointStatistics statistics = EndpointStatistics.builder()
                .p99(P_99 + 1)
                .oneMin(RATE + 0.01)
                .errorRate(ERROR_PROPORTION * RATE)
                .build();
        HealthStatus healthStatus = getHealthStatus(feedbackHandler, statistics);
        assertThat(healthStatus).isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    public void reportStatusIsUnhealthyIfQuietP99IsAboveThreshold() {
        FeedbackHandler feedbackHandler = FeedbackHandler.createForTests();
        EndpointStatistics statistics = EndpointStatistics.builder()
                .p99(QUIET_P99 + 1)
                .oneMin(RATE - 0.01)
                .errorRate(0)
                .build();
        HealthStatus healthStatus = getHealthStatus(feedbackHandler, statistics);
        assertThat(healthStatus).isEqualTo(HealthStatus.UNHEALTHY);
    }

    public HealthStatus getHealthStatus(FeedbackHandler feedbackHandler, EndpointStatistics statistics) {
        return feedbackHandler.getHealthStatusForMetric(
                CLIENT_1, statistics, LEADER_TIME, RATE, P_99, QUIET_P99, ERROR_PROPORTION);
    }
}
