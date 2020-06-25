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

    long maxAcceptableLeaderTimeP99Milli = Constants.MAX_ACCEPTABLE_LEADER_TIME_P99_MILLI.toMillis();
    long maxAcceptableStartTxnTime = Constants.MAX_ACCEPTABLE_START_TXN_P99_MILLI.toMillis();

    private static TimeLockClientFeedbackSink getTimeLockClientFeedbackSink() {
        return TimeLockClientFeedbackSink
                .create(Caffeine.newBuilder()
                        .expireAfterWrite(Constants.HEALTH_FEEDBACK_REPORT_EXPIRATION_MINUTES, TimeUnit.MINUTES)
                        .ticker(FAKE_TICKER)
                        .build());
    }

    // TimeLock Level analysis
    @Test
    public void timeLockIsHealthyIfNoFeedbackIsRegistered() {
        assertThat(FeedbackProvider.getTimeLockHealthStatus(getTimeLockClientFeedbackSink()))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void timeLockIsHealthyIfAllClientsAreHealthy() {
        TimeLockClientFeedbackSink timeLockClientFeedbackSink = getTimeLockClientFeedbackSink();

        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID()));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);

        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void timeLockIsUnhealthyIfAllClientsAreUnhealthy() {
        TimeLockClientFeedbackSink timeLockClientFeedbackSink = getTimeLockClientFeedbackSink();

        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getUnhealthyClientFeedbackReport(CLIENT, UUID.randomUUID()));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);

        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    public void timeLockIsHealthyIfAllClientStatusesAreUnknown() {
        TimeLockClientFeedbackSink timeLockClientFeedbackSink = getTimeLockClientFeedbackSink();

        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getUnknownClientFeedbackReport(CLIENT, UUID.randomUUID()));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);

        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void timeLockIsHealthyIfLessThanThresholdClientStatusesAreUnHealthy() {
        TimeLockClientFeedbackSink timeLockClientFeedbackSink = getTimeLockClientFeedbackSink();

        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID()),
                getHealthyClientFeedbackReport(CLIENT_2, UUID.randomUUID()),
                getUnhealthyClientFeedbackReport(CLIENT_3, UUID.randomUUID()));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);

        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void timeLockIsUnhealthyIfMoreThanThresholdClientStatusesAreUnHealthy() {
        TimeLockClientFeedbackSink timeLockClientFeedbackSink = getTimeLockClientFeedbackSink();

        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID()),
                getUnhealthyClientFeedbackReport(CLIENT_2, UUID.randomUUID()),
                getUnhealthyClientFeedbackReport(CLIENT_3, UUID.randomUUID()));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);

        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.UNHEALTHY);
    }

    // Client Level analysis
    @Test
    public void serviceIsHealthyIfMajorityNodesAreHealthy() {
        TimeLockClientFeedbackSink timeLockClientFeedbackSink = getTimeLockClientFeedbackSink();

        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID()),
                getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID()),
                getUnhealthyClientFeedbackReport(CLIENT, UUID.randomUUID()));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);

        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void serviceIsUnhealthyIfMajorityNodesAreUnhealthy() {
        TimeLockClientFeedbackSink timeLockClientFeedbackSink = getTimeLockClientFeedbackSink();
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID()),
                getUnhealthyClientFeedbackReport(CLIENT, UUID.randomUUID()),
                getUnhealthyClientFeedbackReport(CLIENT, UUID.randomUUID()));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);

        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    public void fallbackToHealthyIfThereIsNoMajority() {
        TimeLockClientFeedbackSink timeLockClientFeedbackSink = getTimeLockClientFeedbackSink();

        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID()),
                getUnhealthyClientFeedbackReport(CLIENT, UUID.randomUUID()));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);

        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    // node level analysis
    @Test
    public void nodeIsHealthyIfMajorityReportsHealthy() {
        TimeLockClientFeedbackSink timeLockClientFeedbackSink = getTimeLockClientFeedbackSink();

        UUID nodeId = UUID.randomUUID();
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT, nodeId),
                getHealthyClientFeedbackReport(CLIENT, nodeId),
                getUnhealthyClientFeedbackReport(CLIENT, nodeId));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);

        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void nodeIsUnhealthyIfMajorityReportsUnhealthy() {
        TimeLockClientFeedbackSink timeLockClientFeedbackSink = getTimeLockClientFeedbackSink();

        UUID nodeId = UUID.randomUUID();
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT, nodeId),
                getReportWithLeaderTimeMetricInUnhealthyState(CLIENT, nodeId),
                getReportWithStartTxnMetricInUnHealthyState(CLIENT, nodeId));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);

        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    public void fallbackToHealthyIfNoMajority() {
        TimeLockClientFeedbackSink timeLockClientFeedbackSink = getTimeLockClientFeedbackSink();

        UUID nodeId = UUID.randomUUID();
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports = ImmutableList.of(
                getUnknownClientFeedbackReport(CLIENT, nodeId),
                getUnknownClientFeedbackReport(CLIENT, nodeId),
                getUnhealthyClientFeedbackReport(CLIENT, nodeId));
        trackedFeedbackReports.forEach(timeLockClientFeedbackSink::registerFeedback);

        assertThat(FeedbackProvider.getTimeLockHealthStatus(timeLockClientFeedbackSink))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    // point analysis
    @Test
    public void reportIsHealthyIfAllMetricsAreHealthy() {
        assertThat(FeedbackProvider.pointFeedbackHealthStatus(
                getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID())))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void reportIsUnknownIfEvenOneMetricIsInUnknownState() {
        assertThat(FeedbackProvider.pointFeedbackHealthStatus(
                getReportWithLeaderTimeMetricInUnknownState(CLIENT, UUID.randomUUID())))
                .isEqualTo(HealthStatus.UNKNOWN);


        assertThat(FeedbackProvider.pointFeedbackHealthStatus(
                getReportWithStartTxnMetricInUnknownState(CLIENT, UUID.randomUUID())))
                .isEqualTo(HealthStatus.UNKNOWN);
    }

    @Test
    public void reportIsUnhealthyIfEvenOneMetricIsInUnhealthy() {
        assertThat(FeedbackProvider.pointFeedbackHealthStatus(
                getReportWithLeaderTimeMetricInUnhealthyState(CLIENT, UUID.randomUUID())))
                .isEqualTo(HealthStatus.UNHEALTHY);


        assertThat(FeedbackProvider.pointFeedbackHealthStatus(
                getReportWithStartTxnMetricInUnHealthyState(CLIENT, UUID.randomUUID())))
                .isEqualTo(HealthStatus.UNHEALTHY);
    }

    // utils
    private ConjureTimeLockClientFeedback getUnhealthyClientFeedbackReport(String serviceName, UUID nodeId) {
        return getClientFeedbackReport(serviceName,
                nodeId,
                Constants.MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE + 1,
                maxAcceptableLeaderTimeP99Milli + 1,
                Constants.MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE + 1,
                maxAcceptableStartTxnTime + 1);
    }

    private ConjureTimeLockClientFeedback getHealthyClientFeedbackReport(String serviceName, UUID nodeId) {
        return getClientFeedbackReport(serviceName,
                nodeId,
                Constants.MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE + 1,
                maxAcceptableLeaderTimeP99Milli - 1,
                Constants.MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE + 1,
                maxAcceptableStartTxnTime - 1);
    }

    private ConjureTimeLockClientFeedback getUnknownClientFeedbackReport(String serviceName, UUID nodeId) {
        return getClientFeedbackReport(serviceName,
                nodeId,
                Constants.MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE - 1,
                maxAcceptableLeaderTimeP99Milli - 1,
                Constants.MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE - 1,
                maxAcceptableStartTxnTime - 1);
    }

    private ConjureTimeLockClientFeedback getReportWithLeaderTimeMetricInUnknownState(String serviceName, UUID nodeId) {
        return getClientFeedbackReport(serviceName,
                nodeId,
                Constants.MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE - 1,
                maxAcceptableLeaderTimeP99Milli - 1,
                Constants.MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE + 1,
                maxAcceptableStartTxnTime - 1);
    }

    private ConjureTimeLockClientFeedback getReportWithStartTxnMetricInUnknownState(String serviceName, UUID nodeId) {
        return getClientFeedbackReport(serviceName,
                nodeId,
                Constants.MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE + 1,
                maxAcceptableLeaderTimeP99Milli - 1,
                Constants.MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE - 1,
                maxAcceptableStartTxnTime - 1);
    }

    private ConjureTimeLockClientFeedback getReportWithLeaderTimeMetricInUnhealthyState(String serviceName, UUID nodeId) {
        return getClientFeedbackReport(serviceName,
                nodeId,
                Constants.MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE + 1,
                maxAcceptableLeaderTimeP99Milli + 1,
                Constants.MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE + 1,
                maxAcceptableStartTxnTime - 1);
    }

    private ConjureTimeLockClientFeedback getReportWithStartTxnMetricInUnHealthyState(String serviceName, UUID nodeId) {
        return getClientFeedbackReport(serviceName,
                nodeId,
                Constants.MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE + 1,
                maxAcceptableLeaderTimeP99Milli - 1,
                Constants.MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE + 1,
                maxAcceptableStartTxnTime + 1);
    }

    private ConjureTimeLockClientFeedback getClientFeedbackReport(String serviceName, UUID nodeId,
            long leaderTimeMinRate, long leaderTimeP99, long startTxnMinRate, long startTxnP99) {
        return ConjureTimeLockClientFeedback.builder()
                .nodeId(nodeId)
                .serviceName(serviceName)
                .atlasVersion("0.1.0")
                .leaderTime(EndpointStatistics
                        .builder()
                        .oneMin(leaderTimeMinRate)
                        .p99(leaderTimeP99)
                        .build())
                .startTransaction(EndpointStatistics
                        .builder()
                        .oneMin(startTxnMinRate)
                        .p99(startTxnP99)
                        .build())
                .build();
    }
}
