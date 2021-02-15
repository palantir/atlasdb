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

import com.google.common.collect.ImmutableList;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;
import com.palantir.timelock.feedback.EndpointStatistics;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.Test;

public class FeedbackAnalysisTest {
    private static final String CLIENT = "client_1";
    private static final String CLIENT_2 = "client_2";
    private static final String CLIENT_3 = "client_3";

    private static final long LEADER_TIME_MAX_P99 = Constants.LEADER_TIME_SERVICE_LEVEL_OBJECTIVES
            .maximumPermittedSteadyStateP99()
            .toNanos();
    private static final long START_TRANSACTION_MAX_P99 = Constants.START_TRANSACTION_SERVICE_LEVEL_OBJECTIVES
            .maximumPermittedSteadyStateP99()
            .toNanos();
    private static final double LEADER_TIME_MIN_RATE =
            Constants.LEADER_TIME_SERVICE_LEVEL_OBJECTIVES.minimumRequestRateForConsideration();
    private static final double START_TXN_MIN_RATE =
            Constants.START_TRANSACTION_SERVICE_LEVEL_OBJECTIVES.minimumRequestRateForConsideration();
    private static final long START_TXN_QUIET_P99_LIMIT = Constants.START_TRANSACTION_SERVICE_LEVEL_OBJECTIVES
            .maximumPermittedQuietP99()
            .toNanos();

    // TimeLock Level analysis
    @Test
    public void timeLockIsHealthyIfNoFeedbackIsRegistered() {
        FeedbackHandler feedbackHandler = getFeedbackHandlerWithReports(ImmutableList.of());

        assertThat(feedbackHandler.getTimeLockHealthStatus().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    private FeedbackHandler getFeedbackHandlerWithReports(ImmutableList<ConjureTimeLockClientFeedback> reports) {
        FeedbackHandler feedbackHandler = FeedbackHandler.createForTests();
        reports.forEach(feedbackHandler::handle);
        return feedbackHandler;
    }

    @Test
    public void timeLockIsHealthyIfAllClientsAreHealthy() {
        FeedbackHandler feedbackHandler = getFeedbackHandlerWithReports(
                ImmutableList.of(getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID())));

        assertThat(feedbackHandler.getTimeLockHealthStatus().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void timeLockIsHealthyIfAllClientStatusesAreUnknown() {
        FeedbackHandler feedbackHandler = getFeedbackHandlerWithReports(
                ImmutableList.of(getUnknownClientFeedbackReport(CLIENT, UUID.randomUUID())));

        assertThat(feedbackHandler.getTimeLockHealthStatus().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void timeLockIsHealthyIfLessThanFixedThresholdClientStatusesAreUnHealthy() {
        FeedbackHandler feedbackHandler = getFeedbackHandlerWithReports(ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID()),
                getHealthyClientFeedbackReport(CLIENT_2, UUID.randomUUID()),
                getUnhealthyClientFeedbackReport(CLIENT_3, UUID.randomUUID())));

        assertThat(feedbackHandler.getTimeLockHealthStatus().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void timeLockIsHealthyIfLessThanSpecifiedRatioOfClientsAreUnhealthy() {
        FeedbackHandler feedbackHandler = FeedbackHandler.createForTests();
        IntStream.range(1, 5)
                .forEach(index ->
                        feedbackHandler.handle(getUnhealthyClientFeedbackReport("Client_" + index, UUID.randomUUID())));
        IntStream.range(5, 16)
                .forEach(index ->
                        feedbackHandler.handle(getHealthyClientFeedbackReport("Client_" + index, UUID.randomUUID())));

        assertThat(feedbackHandler.getTimeLockHealthStatus().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void timeLockIsUnhealthyIfMoreThanSpecifiedRatioOfClientsAreUnhealthy() {
        FeedbackHandler feedbackHandler = FeedbackHandler.createForTests();

        IntStream.range(1, 10)
                .forEach(index ->
                        feedbackHandler.handle(getUnhealthyClientFeedbackReport("Client_" + index, UUID.randomUUID())));
        IntStream.range(10, 24)
                .forEach(index ->
                        feedbackHandler.handle(getHealthyClientFeedbackReport("Client_" + index, UUID.randomUUID())));

        assertThat(feedbackHandler.getTimeLockHealthStatus().status()).isEqualTo(HealthStatus.UNHEALTHY);
    }

    // Client Level analysis
    @Test
    public void serviceIsHealthyIfMajorityNodesAreHealthy() {
        FeedbackHandler feedbackHandler = getFeedbackHandlerWithReports(ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID()),
                getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID()),
                getUnhealthyClientFeedbackReport(CLIENT, UUID.randomUUID())));

        assertThat(feedbackHandler.getTimeLockHealthStatus().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void serviceIsUnhealthyIfMajorityNodesAreUnhealthy() {
        FeedbackHandler feedbackHandler = getFeedbackHandlerWithReports(ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID()),
                getUnhealthyClientFeedbackReport(CLIENT, UUID.randomUUID()),
                getUnhealthyClientFeedbackReport(CLIENT, UUID.randomUUID()),
                getUnhealthyClientFeedbackReport(CLIENT_2, UUID.randomUUID()),
                getUnhealthyClientFeedbackReport(CLIENT_3, UUID.randomUUID())));

        assertThat(feedbackHandler.getTimeLockHealthStatus().status()).isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    public void fallbackToHealthyIfThereIsNoMajority() {
        FeedbackHandler feedbackHandler = getFeedbackHandlerWithReports(ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID()),
                getUnhealthyClientFeedbackReport(CLIENT, UUID.randomUUID())));

        assertThat(feedbackHandler.getTimeLockHealthStatus().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    // node level analysis
    @Test
    public void nodeIsHealthyIfMajorityReportsHealthy() {
        UUID nodeId = UUID.randomUUID();
        FeedbackHandler feedbackHandler = getFeedbackHandlerWithReports(ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT, nodeId),
                getHealthyClientFeedbackReport(CLIENT, nodeId),
                getUnhealthyClientFeedbackReport(CLIENT, nodeId)));

        assertThat(feedbackHandler.getTimeLockHealthStatus().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void nodeIsUnhealthyIfMajorityReportsAreUnhealthy() {
        UUID nodeId = UUID.randomUUID();
        FeedbackHandler feedbackHandler = getFeedbackHandlerWithReports(ImmutableList.of(
                getHealthyClientFeedbackReport(CLIENT, nodeId),
                getUnhealthyClientFeedbackReport(CLIENT, nodeId),
                getReportWithLeaderTimeMetricInUnhealthyState(CLIENT, nodeId),
                getReportWithStartTxnMetricInUnHealthyState(CLIENT_2, nodeId),
                getReportWithStartTxnMetricInUnHealthyState(CLIENT_3, nodeId)));

        assertThat(feedbackHandler.getTimeLockHealthStatus().status()).isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    public void fallbackToHealthyIfNoMajority() {
        UUID nodeId = UUID.randomUUID();
        FeedbackHandler feedbackHandler = getFeedbackHandlerWithReports(ImmutableList.of(
                getUnknownClientFeedbackReport(CLIENT, nodeId),
                getUnknownClientFeedbackReport(CLIENT, nodeId),
                getUnhealthyClientFeedbackReport(CLIENT, nodeId)));

        assertThat(feedbackHandler.getTimeLockHealthStatus().status()).isEqualTo(HealthStatus.HEALTHY);
    }

    // point analysis
    @Test
    public void reportIsHealthyIfAllMetricsAreHealthy() {
        FeedbackHandler feedbackHandler = FeedbackHandler.createForTests();
        assertThat(feedbackHandler.pointFeedbackHealthStatus(getHealthyClientFeedbackReport(CLIENT, UUID.randomUUID())))
                .isEqualTo(HealthStatus.HEALTHY);
    }

    @Test
    public void reportIsUnknownIfEvenOneMetricIsInUnknownState() {
        FeedbackHandler feedbackHandler = FeedbackHandler.createForTests();
        assertThat(feedbackHandler.pointFeedbackHealthStatus(
                        getReportWithLeaderTimeMetricInUnknownState(CLIENT, UUID.randomUUID())))
                .isEqualTo(HealthStatus.UNKNOWN);

        assertThat(feedbackHandler.pointFeedbackHealthStatus(
                        getReportWithStartTxnMetricInUnknownState(CLIENT, UUID.randomUUID())))
                .isEqualTo(HealthStatus.UNKNOWN);
    }

    @Test
    public void reportIsUnhealthyIfP99IsOutlier() {
        FeedbackHandler feedbackHandler = FeedbackHandler.createForTests();
        assertThat(feedbackHandler.pointFeedbackHealthStatus(
                        getReportWithStartTxnForVeryHighP99(CLIENT, UUID.randomUUID())))
                .isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    public void reportIsUnhealthyIfEvenOneMetricIsInUnhealthy() {
        FeedbackHandler feedbackHandler = FeedbackHandler.createForTests();
        assertThat(feedbackHandler.pointFeedbackHealthStatus(
                        getReportWithLeaderTimeMetricInUnhealthyState(CLIENT, UUID.randomUUID())))
                .isEqualTo(HealthStatus.UNHEALTHY);

        assertThat(feedbackHandler.pointFeedbackHealthStatus(
                        getReportWithStartTxnMetricInUnHealthyState(CLIENT, UUID.randomUUID())))
                .isEqualTo(HealthStatus.UNHEALTHY);
    }

    @Test
    public void isAbleToHandleReportsWhereLeaderTimeAndStartTransactionAreEqual() {
        FeedbackHandler feedbackHandler = FeedbackHandler.createForTests();
        ConjureTimeLockClientFeedback report = getClientFeedbackReport(CLIENT, UUID.randomUUID(), 0, 0, 0, 0);

        assertThat(feedbackHandler.pointFeedbackHealthStatus(report)).isEqualTo(HealthStatus.UNKNOWN);
    }

    // utils
    private ConjureTimeLockClientFeedback getUnhealthyClientFeedbackReport(String serviceName, UUID nodeId) {
        return getClientFeedbackReport(
                serviceName,
                nodeId,
                LEADER_TIME_MIN_RATE + 1,
                LEADER_TIME_MAX_P99 + 1,
                START_TXN_MIN_RATE + 1,
                START_TRANSACTION_MAX_P99 + 1);
    }

    private ConjureTimeLockClientFeedback getHealthyClientFeedbackReport(String serviceName, UUID nodeId) {
        return getClientFeedbackReport(
                serviceName,
                nodeId,
                LEADER_TIME_MIN_RATE + 1,
                LEADER_TIME_MAX_P99 - 1,
                START_TXN_MIN_RATE + 1,
                START_TRANSACTION_MAX_P99 - 1);
    }

    private ConjureTimeLockClientFeedback getUnknownClientFeedbackReport(String serviceName, UUID nodeId) {
        return getClientFeedbackReport(
                serviceName,
                nodeId,
                LEADER_TIME_MIN_RATE - 1,
                LEADER_TIME_MAX_P99 - 1,
                START_TXN_MIN_RATE - 1,
                START_TRANSACTION_MAX_P99 - 1);
    }

    private ConjureTimeLockClientFeedback getReportWithLeaderTimeMetricInUnknownState(String serviceName, UUID nodeId) {
        return getClientFeedbackReport(
                serviceName,
                nodeId,
                LEADER_TIME_MIN_RATE - 1,
                LEADER_TIME_MAX_P99 - 1,
                START_TXN_MIN_RATE + 1,
                START_TRANSACTION_MAX_P99 - 1);
    }

    private ConjureTimeLockClientFeedback getReportWithStartTxnMetricInUnknownState(String serviceName, UUID nodeId) {
        return getClientFeedbackReport(
                serviceName,
                nodeId,
                LEADER_TIME_MIN_RATE + 1,
                LEADER_TIME_MAX_P99 - 1,
                START_TXN_MIN_RATE - 1,
                START_TRANSACTION_MAX_P99 - 1);
    }

    private ConjureTimeLockClientFeedback getReportWithLeaderTimeMetricInUnhealthyState(
            String serviceName, UUID nodeId) {
        return getClientFeedbackReport(
                serviceName,
                nodeId,
                LEADER_TIME_MIN_RATE + 1,
                LEADER_TIME_MAX_P99 + 1,
                START_TXN_MIN_RATE + 1,
                START_TRANSACTION_MAX_P99 - 1);
    }

    private ConjureTimeLockClientFeedback getReportWithStartTxnMetricInUnHealthyState(String serviceName, UUID nodeId) {
        return getClientFeedbackReport(
                serviceName,
                nodeId,
                LEADER_TIME_MIN_RATE + 1,
                LEADER_TIME_MAX_P99 - 1,
                START_TXN_MIN_RATE + 1,
                START_TRANSACTION_MAX_P99 + 1);
    }

    private ConjureTimeLockClientFeedback getReportWithStartTxnForVeryHighP99(String serviceName, UUID nodeId) {
        return getClientFeedbackReport(
                serviceName,
                nodeId,
                LEADER_TIME_MIN_RATE + 1,
                LEADER_TIME_MAX_P99 - 1,
                START_TXN_MIN_RATE - 0.001, // Outliers are bad, even if req rate is low
                START_TXN_QUIET_P99_LIMIT + 1);
    }

    private ConjureTimeLockClientFeedback getClientFeedbackReport(
            String serviceName,
            UUID nodeId,
            double leaderTimeMinRate,
            double leaderTimeP99,
            double startTxnMinRate,
            double startTxnP99) {
        return ConjureTimeLockClientFeedback.builder()
                .nodeId(nodeId)
                .serviceName(serviceName)
                .atlasVersion("0.1.0")
                .leaderTime(EndpointStatistics.builder()
                        .oneMin(leaderTimeMinRate)
                        .p99(leaderTimeP99)
                        .build())
                .startTransaction(EndpointStatistics.builder()
                        .oneMin(startTxnMinRate)
                        .p99(startTxnP99)
                        .build())
                .build();
    }
}
