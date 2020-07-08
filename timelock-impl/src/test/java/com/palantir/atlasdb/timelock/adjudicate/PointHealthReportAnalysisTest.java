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

import org.junit.Test;

import com.palantir.timelock.feedback.EndpointStatistics;

public class PointHealthReportAnalysisTest {
    private static final String CLIENT_1 = "Client1";

    private static final double RATE =
            Constants.LEADER_TIME_SERVICE_LEVEL_OBJECTIVES.minimumRequestRateForConsideration();
    private static final long P_99 =
            Constants.LEADER_TIME_SERVICE_LEVEL_OBJECTIVES.maximumPermittedP99().toNanos();
    private static final double ERROR_PROPORTION =
            Constants.LEADER_TIME_SERVICE_LEVEL_OBJECTIVES.maximumPermittedErrorProportion();

    @Test
    public void reportIsHealthyWhenEverythingIsRight() {
        FeedbackHandler feedbackHandler = new FeedbackHandler();
        EndpointStatistics statistics = EndpointStatistics
                .builder()
                .p99(P_99 - 1)
                .oneMin(RATE + 1)
                .errorRate(0)
                .build();
        PointHealthStatusReport healthStatusForMetric = feedbackHandler.getHealthStatusForMetric(
                CLIENT_1,
                statistics,
                Constants.LEADER_TIME_SERVICE_LEVEL_OBJECTIVES);
        assertThat(healthStatusForMetric.status()).isEqualTo(HealthStatus.HEALTHY);
        assertThat(healthStatusForMetric.message().isPresent()).isFalse();
    }

    @Test
    public void reportStatusIsUnknownIfReqRateIsBelowThreshold() {
        FeedbackHandler feedbackHandler = new FeedbackHandler();
        EndpointStatistics statistics = EndpointStatistics
                .builder()
                .p99(P_99 + 1)
                .oneMin(RATE - 0.01)
                .errorRate(ERROR_PROPORTION * RATE + 0.1)
                .build();
        PointHealthStatusReport healthStatusForMetric =
                feedbackHandler.getHealthStatusForMetric(CLIENT_1,
                        statistics,
                        Constants.LEADER_TIME_SERVICE_LEVEL_OBJECTIVES);
        assertThat(healthStatusForMetric.status()).isEqualTo(HealthStatus.UNKNOWN);
        assertThat(healthStatusForMetric.message().isPresent()).isTrue();
        assertThat(healthStatusForMetric.message().get())
                .isEqualTo("[Client1] | Point health status for leaderTime is UNKNOWN as request rate is low - 0.99");
    }

    @Test
    public void reportStatusIsUnhealthyIfErrProportionIsHigh() {
        FeedbackHandler feedbackHandler = new FeedbackHandler();
        EndpointStatistics statistics = EndpointStatistics
                .builder()
                .p99(P_99 + 1)
                .oneMin(RATE + 0.01)
                .errorRate(ERROR_PROPORTION * RATE + 0.1)
                .build();
        PointHealthStatusReport healthStatusForMetric =
                feedbackHandler.getHealthStatusForMetric(CLIENT_1,
                        statistics,
                        Constants.LEADER_TIME_SERVICE_LEVEL_OBJECTIVES);
        assertThat(healthStatusForMetric.status()).isEqualTo(HealthStatus.UNHEALTHY);
        assertThat(healthStatusForMetric.message().isPresent()).isTrue();
        assertThat(healthStatusForMetric.message().get())
                .isEqualTo("[Client1] | Point health status for leaderTime is UNHEALTHY due to high error proportion - 0.59");
    }

    @Test
    public void reportStatusIsUnhealthyIfP99IsAboveThreshold() {
        FeedbackHandler feedbackHandler = new FeedbackHandler();
        EndpointStatistics statistics = EndpointStatistics
                .builder()
                .p99(P_99 + 1)
                .oneMin(RATE + 0.01)
                .errorRate(ERROR_PROPORTION * RATE)
                .build();
        PointHealthStatusReport healthStatusForMetric =
                feedbackHandler.getHealthStatusForMetric(CLIENT_1,
                        statistics,
                        Constants.LEADER_TIME_SERVICE_LEVEL_OBJECTIVES);
        assertThat(healthStatusForMetric.status()).isEqualTo(HealthStatus.UNHEALTHY);
        assertThat(healthStatusForMetric.message().isPresent()).isTrue();
        assertThat(healthStatusForMetric.message().get())
                .isEqualTo("[Client1] | Point health status for leaderTime is UNHEALTHY due to high p99 - 200000001.00ns");
    }
}
