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
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableList;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;
import com.palantir.timelock.feedback.EndpointStatistics;

public class FeedbackAnalysisTest {
    private static final FakeTicker FAKE_TICKER = new FakeTicker();
    private static final String CLIENT = "client_1";
    private static final String CLIENT_2 = "client_2";
    private static final String CLIENT_3 = "client_3";


    private static TimeLockClientFeedbackSink timeLockClientFeedbackSink = TimeLockClientFeedbackSink
            .create(Caffeine.newBuilder()
                    .expireAfterWrite(Constants.HEALTH_FEEDBACK_REPORT_EXPIRATION_MINUTES, TimeUnit.MINUTES)
                    .ticker(FAKE_TICKER)
                    .build());

    @After
    public void resetCache() {
        FAKE_TICKER.advance(Constants.HEALTH_FEEDBACK_REPORT_EXPIRATION_MINUTES, TimeUnit.MINUTES);
    }

    // TimeLock Level analysis
    @Test
    public void timeLockIsHealthyIfNoFeedbackIsRegister() {
        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void timeLockIsHealthyIfAllClientsAreHealthy() {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);
        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void timeLockIsHealthyIfAllClientsAreUnhealthy() {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getUnhealthyClientFeedbackReport(CLIENT));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);
        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    public void timeLockIsHealthyIfAllClientStatusesAreUnknown() {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getUnknownClientFeedbackReport(CLIENT));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);
        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void timeLockIsHealthyIfThresholdClientStatusesAreHealthy() {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT),
                getHealthyClientFeedbackReport(CLIENT_2),
                getUnhealthyClientFeedbackReport(CLIENT_3));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);
        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void timeLockIsUnhealthyIfThresholdClientStatusesAreUnHealthy() {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT),
                getUnhealthyClientFeedbackReport(CLIENT_2),
                getUnhealthyClientFeedbackReport(CLIENT_3));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);
        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.UNHEALTHY);
    }

    // Client Level analysis
    @Test
    public void serviceIsHealthyIfMajorityClientsHealthy() {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT),
                getHealthyClientFeedbackReport(CLIENT),
                getUnhealthyClientFeedbackReport(CLIENT));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);
        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void serviceIsUnhealthyIfMajorityClientsUnhealthy() {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT),
                getUnhealthyClientFeedbackReport(CLIENT),
                getUnhealthyClientFeedbackReport(CLIENT));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);
        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    public void fallbackToHealthyIfThereIsNoMajority() {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT),
                getUnhealthyClientFeedbackReport(CLIENT));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);
        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    private ConjureTimeLockClientFeedback getUnhealthyClientFeedbackReport(String serviceName) {
        return getClientFeedbackReport(serviceName, Constants.MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE + 1,
                Constants.MAX_ACCEPTABLE_LEADER_TIME_P99_MILLI + 1,
                Constants.MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE + 1,
                Constants.MAX_ACCEPTABLE_START_TXN_P99_MILLI + 1);
    }

    private ConjureTimeLockClientFeedback getHealthyClientFeedbackReport(String serviceName) {
        return getClientFeedbackReport(serviceName, Constants.MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE + 1,
                Constants.MAX_ACCEPTABLE_LEADER_TIME_P99_MILLI - 1,
                Constants.MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE + 1,
                Constants.MAX_ACCEPTABLE_START_TXN_P99_MILLI - 1);
    }

    private ConjureTimeLockClientFeedback getUnknownClientFeedbackReport(String serviceName) {
        return getClientFeedbackReport(serviceName, Constants.MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE - 1,
                Constants.MAX_ACCEPTABLE_LEADER_TIME_P99_MILLI - 1,
                Constants.MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE - 1,
                Constants.MAX_ACCEPTABLE_START_TXN_P99_MILLI - 1);
    }

    private ConjureTimeLockClientFeedback getClientFeedbackReport(String serviceName, int i1, int i2, int i3, int i4) {
        return ConjureTimeLockClientFeedback.builder()
                .nodeId(UUID.randomUUID())
                .serviceName(serviceName)
                .atlasVersion("0.1.0")
                .leaderTime(EndpointStatistics
                        .builder()
                        .oneMin(i1)
                        .p99(i2)
                        .build())
                .startTransaction(EndpointStatistics
                        .builder()
                        .oneMin(i3)
                        .p99(i4)
                        .build())
                .build();
    }
}