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

import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;
import com.palantir.timelock.feedback.EndpointStatistics;

public class FeedbackAnalysisTest {

    @Test
    public void timeLockIsHealthyIfNoFeedbackIsRegister() {
        assertThat(FeedbackProvider.getTimeLockHealthStatus()).isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void timeLockIsHealthyIfAllClientsAreHealthy() {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(ConjureTimeLockClientFeedback.builder()
                .nodeId(UUID.randomUUID())
                .serviceName("client_1")
                .atlasVersion("0.1.0")
                .leaderTime(EndpointStatistics
                        .builder()
                        .oneMin(Constants.MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE + 1)
                        .p99(Constants.MAX_ACCEPTABLE_LEADER_TIME_P99_MILLI - 1)
                        .build())
                .startTransaction(EndpointStatistics
                        .builder()
                        .oneMin(Constants.MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE + 1)
                        .p99(Constants.MAX_ACCEPTABLE_START_TXN_P99_MILLI - 1)
                        .build())
                .build());
        assertThat(FeedbackProvider.healthStateOfTimeLock(
                FeedbackProvider
                        .organizeFeedbackReports(trackedFeedbackReports)))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void timeLockIsHealthyIfAllClientsAreUnhealthy() {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(ConjureTimeLockClientFeedback.builder()
                .nodeId(UUID.randomUUID())
                .serviceName("client_1")
                .atlasVersion("0.1.0")
                .leaderTime(EndpointStatistics
                        .builder()
                        .oneMin(Constants.MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE + 1)
                        .p99(Constants.MAX_ACCEPTABLE_LEADER_TIME_P99_MILLI + 1)
                        .build())
                .startTransaction(EndpointStatistics
                        .builder()
                        .oneMin(Constants.MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE + 1)
                        .p99(Constants.MAX_ACCEPTABLE_START_TXN_P99_MILLI + 1)
                        .build())
                .build());
        assertThat(FeedbackProvider.healthStateOfTimeLock(
                FeedbackProvider
                        .organizeFeedbackReports(trackedFeedbackReports)))
                .isEqualTo(HealthStatus.UNHEALTHY);
    }
}