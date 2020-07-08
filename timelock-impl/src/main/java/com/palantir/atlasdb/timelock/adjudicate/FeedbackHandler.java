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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.Client;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;
import com.palantir.timelock.feedback.EndpointStatistics;

public class FeedbackHandler {
    private static final Logger log = LoggerFactory.getLogger(FeedbackHandler.class);

    private final TimeLockClientFeedbackSink timeLockClientFeedbackSink = TimeLockClientFeedbackSink
            .create(Caffeine
            .newBuilder()
            .expireAfterWrite(Constants.HEALTH_FEEDBACK_REPORT_EXPIRATION_MINUTES.toMinutes(), TimeUnit.MINUTES)
                        .build());

    public void handle(ConjureTimeLockClientFeedback feedback) {
        timeLockClientFeedbackSink.registerFeedback(feedback);
    }

    public HealthStatusReport getTimeLockHealthStatus() {
        Map<String, ServiceFeedback> organizedFeedback =
                organizeFeedbackReportsByService(timeLockClientFeedbackSink.getTrackedFeedbackReports());

        return healthStateOfTimeLock(organizedFeedback);
    }

    private Map<String, ServiceFeedback> organizeFeedbackReportsByService(
            List<ConjureTimeLockClientFeedback> trackedFeedbackReports) {
        Map<String, ServiceFeedback> serviceWiseOrganizedFeedback = Maps.newHashMap();

        for (ConjureTimeLockClientFeedback feedback : trackedFeedbackReports) {
            ServiceFeedback feedbackForService = serviceWiseOrganizedFeedback.computeIfAbsent(
                    feedback.getServiceName(), service -> new ServiceFeedback());

            feedbackForService.addFeedbackForNode(feedback.getNodeId(), feedback);
        }
        return serviceWiseOrganizedFeedback;
    }

    private HealthStatusReport healthStateOfTimeLock(
            Map<String, ServiceFeedback> organizedFeedbackByServiceName) {
        int maxAllowedUnhealthyServices = getMaxAllowedUnhealthyServices(organizedFeedbackByServiceName.size());

        List<Client> unhealthyClients = KeyedStream.stream(organizedFeedbackByServiceName)
                .filterEntries((serviceName, serviceFeedback) ->
                        getHealthStatusForService(serviceFeedback) == HealthStatus.UNHEALTHY)
                .keys()
                .map(serviceName -> Client.of(serviceName))
                .collect(Collectors.toList());

        if (unhealthyClients.size() > maxAllowedUnhealthyServices) {
            return ImmutableHealthStatusReport
                    .builder()
                    .status(HealthStatus.UNHEALTHY)
                    .unhealthyClients(unhealthyClients)
                    .message(String.format("TimeLock is unhealthy as %d of %d clients are unhealthy"
                            + ". The highest acceptable number of unhealthy clients is - %d",
                            unhealthyClients.size(),
                            organizedFeedbackByServiceName.size(),
                            maxAllowedUnhealthyServices))
                    .build();
        }
        return ImmutableHealthStatusReport.builder().status(HealthStatus.HEALTHY).build();
    }

    private int getMaxAllowedUnhealthyServices(int numberOfServices) {
        return Math.max((numberOfServices * Constants.UNHEALTHY_CLIENTS_PROPORTION_LIMIT.getNumerator())
                / Constants.UNHEALTHY_CLIENTS_PROPORTION_LIMIT.getDenominator(), Constants.MIN_UNHEALTHY_SERVICES);
    }

    private HealthStatus getHealthStatusForService(ServiceFeedback serviceFeedback) {
        // only the status that appears majority number of times is considered,
        // otherwise the health status for service is 'unknown'

        return getHealthStatusOfMajority(serviceFeedback.values(),
                this::getHealthStatusForNode);
    }

    private HealthStatus getHealthStatusForNode(List<ConjureTimeLockClientFeedback> feedbackForNode) {
        // only the status that appears majority number of times is considered,
        // otherwise the health status for node is 'unknown'

        return getHealthStatusOfMajority(feedbackForNode,
                this::pointFeedbackHealthStatus);
    }

    private <T> HealthStatus getHealthStatusOfMajority(Collection<T> feedbacks,
            Function<T, HealthStatus> mapper) {
        int majorityThreshold = (feedbacks.size() / 2) + 1;
        return KeyedStream.stream(Utils.getFrequencyMap(feedbacks.stream()
                .map(mapper)))
                .filterEntries((key, val) -> val >= majorityThreshold)
                .keys()
                .findFirst()
                .orElse(HealthStatus.UNKNOWN);
    }

    @VisibleForTesting
    HealthStatus pointFeedbackHealthStatus(ConjureTimeLockClientFeedback healthReport) {
        if (Constants.ATLAS_BLACKLISTED_VERSIONS.contains(healthReport.getAtlasVersion())) {
            return HealthStatus.UNKNOWN;
        }

        // considering the worst performing metric only, the health check should fail even if one end-point is unhealthy
        return KeyedStream.ofEntries(Stream.of(
                Maps.immutableEntry(healthReport.getLeaderTime(), Constants.LEADER_TIME_SERVICE_LEVEL_OBJECTIVES),
                Maps.immutableEntry(healthReport.getStartTransaction(),
                        Constants.START_TRANSACTION_SERVICE_LEVEL_OBJECTIVES)))
                .filterKeys(Optional::isPresent)
                .mapKeys(Optional::get)
                .map((userStats, sloSpec) -> getHealthStatusForMetric(userStats,
                        sloSpec.name(),
                        sloSpec.minimumRequestRateForConsideration(),
                        sloSpec.maximumPermittedSteadyStateP99().toNanos(),
                        sloSpec.maximumPermittedErrorProportion(),
                        sloSpec.maximumPermittedQuietP99().toNanos()))
                .values()
                .max(HealthStatus.getHealthStatusComparator())
                .orElse(HealthStatus.HEALTHY);
    }

    @VisibleForTesting
    HealthStatus getHealthStatusForMetric(EndpointStatistics endpointStatistics,
            String metric,
            double rateThreshold,
            long p99Limit,
            double errorRateProportion,
            long quietP99Limit) {

        // Outliers indicate badness even with low request rates. The request rate should be greater than
        // zero to counter lingering badness from a single slow request
        if (endpointStatistics.getP99() > quietP99Limit && endpointStatistics.getOneMin() > 0) {
            return HealthStatus.UNHEALTHY;
        }

        double oneMin = endpointStatistics.getOneMin();
        if (oneMin < rateThreshold) {
            log.info("Point health status for {} is UNKNOWN as request rate is low - {}",
                    SafeArg.of("metricName", metric),
                    SafeArg.of("oneMinRate", oneMin));
            return HealthStatus.UNKNOWN;
        }

        double errorProportion = getErrorProportion(endpointStatistics);
        if (errorProportion > errorRateProportion) {
            log.info("Point health status for {} is UNHEALTHY due to high error proportion - {}",
                    SafeArg.of("metricName", metric),
                    SafeArg.of("errorProportion", errorProportion));
            return HealthStatus.UNHEALTHY;
        }

        if (endpointStatistics.getP99() > p99Limit) {
            log.info("Point health status for {} is UNHEALTHY due to high p99 - {}",
                    SafeArg.of("metricName", metric),
                    SafeArg.of("p99", endpointStatistics.getP99()));
            return HealthStatus.UNHEALTHY;
        }

        return HealthStatus.HEALTHY;
    }

    private double getErrorProportion(EndpointStatistics endpointStatistics) {
        double oneMin = endpointStatistics.getOneMin();
        return (oneMin == 0) ? 0 : endpointStatistics.getErrorRate().orElse(Double.MIN_VALUE) / oneMin;
    }
}
