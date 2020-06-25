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
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.palantir.common.streams.KeyedStream;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;
import com.palantir.timelock.feedback.EndpointStatistics;

public final class FeedbackProvider {

    private FeedbackProvider() {
        // no op
    }

    public static HealthStatus getTimeLockHealthStatus(TimeLockClientFeedbackSink timeLockClientFeedbackSink) {
        List<ConjureTimeLockClientFeedback> trackedFeedbackReports =
                timeLockClientFeedbackSink.getTrackedFeedbackReports();

        Map<String, ServiceFeedback> organizedFeedback =
                organizeFeedbackReportsByService(trackedFeedbackReports);

        return healthStateOfTimeLock(organizedFeedback);
    }

    private static Map<String, ServiceFeedback> organizeFeedbackReportsByService(
            List<ConjureTimeLockClientFeedback> trackedFeedbackReports) {
        Map<String, ServiceFeedback> serviceWiseOrganizedFeedback = Maps.newHashMap();

        for (ConjureTimeLockClientFeedback feedback : trackedFeedbackReports) {
            ServiceFeedback feedbackForService = serviceWiseOrganizedFeedback.computeIfAbsent(
                    feedback.getServiceName(), service -> new ServiceFeedback());

            feedbackForService.addFeedbackForNode(feedback.getNodeId(), feedback);
        }
        return serviceWiseOrganizedFeedback;
    }

    private static HealthStatus healthStateOfTimeLock(
            Map<String, ServiceFeedback> organizedFeedbackByServiceName) {
        int maxAllowedUnhealthyServices = (organizedFeedbackByServiceName.size()
                * Constants.UNHEALTHY_CLIENTS_PROPORTION_LIMIT.getNumerator())
                / Constants.UNHEALTHY_CLIENTS_PROPORTION_LIMIT.getDenominator();

        return KeyedStream.stream(organizedFeedbackByServiceName).values().filter(
                serviceFeedback -> getHealthStatusForService(serviceFeedback) == HealthStatus.UNHEALTHY).count()
                > maxAllowedUnhealthyServices ? HealthStatus.UNHEALTHY : HealthStatus.HEALTHY;
    }

    private static HealthStatus getHealthStatusForService(ServiceFeedback serviceFeedback) {
        // only the status that appears majority number of times is considered,
        // otherwise the health status for service is 'unknown'

        return getHealthStatusOfMajority(serviceFeedback.values().stream(),
                FeedbackProvider::getHealthStatusForNode,
                serviceFeedback.numberOfNodes() / 2);
    }

    private static HealthStatus getHealthStatusForNode(List<ConjureTimeLockClientFeedback> feedbackForNode) {
        // only the status that appears majority number of times is considered,
        // otherwise the health status for node is 'unknown'

        return getHealthStatusOfMajority(feedbackForNode.stream(),
                FeedbackProvider::pointFeedbackHealthStatus,
                feedbackForNode.size() / 2);
    }

    private static <T> HealthStatus getHealthStatusOfMajority(Stream<T> feedbackForNode,
            Function<T, HealthStatus> mapper,
            int minThresholdToBeMajority) {
        return KeyedStream.stream(Utils.getFrequencyMap(feedbackForNode
                .map(mapper)))
                .filterEntries((key, val) -> val > minThresholdToBeMajority)
                .keys()
                .findFirst()
                .orElse(HealthStatus.UNKNOWN);
    }

    @VisibleForTesting
    static HealthStatus pointFeedbackHealthStatus(ConjureTimeLockClientFeedback healthReport) {
        if (Constants.ATLAS_BLACKLISTED_VERSIONS.contains(healthReport.getAtlasVersion())) {
            return HealthStatus.UNKNOWN;
        }
        // considering the worst performing metric only, the health check should fail even if one end-point is unhealthy
        HealthStatus healthStatus = HealthStatus.HEALTHY;

        if (healthReport.getLeaderTime().isPresent()) {
            healthStatus = HealthStatus.getWorst(healthStatus,
                    getHealthStatusForService(healthReport.getLeaderTime().get(),
                            Constants.MIN_REQUIRED_LEADER_TIME_ONE_MINUTE_RATE,
                            Constants.MAX_ACCEPTABLE_LEADER_TIME_P99_MILLI.toMillis()));
        }

        if (healthReport.getStartTransaction().isPresent()) {
            healthStatus = HealthStatus.getWorst(healthStatus,
                    getHealthStatusForService(healthReport.getStartTransaction().get(),
                            Constants.MIN_REQUIRED_START_TXN_ONE_MINUTE_RATE,
                            Constants.MAX_ACCEPTABLE_START_TXN_P99_MILLI.toMillis()));
        }

        return healthStatus;
    }

    private static HealthStatus getHealthStatusForService(EndpointStatistics endpointStatistics,
            int rateThreshold,
            long p99Limit) {

        if (endpointStatistics.getOneMin() < rateThreshold) {
            return HealthStatus.UNKNOWN;
        }

        return endpointStatistics.getP99() > p99Limit
                ? HealthStatus.UNHEALTHY : HealthStatus.HEALTHY;
    }
}
