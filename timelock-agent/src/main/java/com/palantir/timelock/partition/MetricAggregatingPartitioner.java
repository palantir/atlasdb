/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.timelock.partition;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.PaxosQuorumChecker;

public class MetricAggregatingPartitioner implements TimeLockPartitioner {
    private static final Logger log = LoggerFactory.getLogger(MetricAggregatingPartitioner.class);

    private final TimeLockPartitioner delegate;
    private final Set<ClientMetricsService> metricsServices;

    public MetricAggregatingPartitioner(TimeLockPartitioner delegate, Set<ClientMetricsService> metricsServices) {
        this.delegate = delegate;
        this.metricsServices = metricsServices;
    }

    @Override
    public Assignment partition(List<String> clients, List<String> hosts, long seed) {
        Map<String, Double> metrics = Maps.newHashMap();
        List<MetricsResponse> remoteResponses = PaxosQuorumChecker.collectAsManyResponsesAsPossible(
                ImmutableList.copyOf(metricsServices),
                service -> ImmutableMetricsResponse.of(true, service.getLoadMetrics()),
                Executors.newFixedThreadPool(hosts.size()),
                5); // timeout in seconds
        List<Map<String, Double>> metricsValues = remoteResponses.stream()
                .map(MetricsResponse::metricsValues)
                .collect(Collectors.toList());

        for (Map<String, Double> metricsValue : metricsValues) {
            for (Map.Entry<String, Double> entry : metricsValue.entrySet()) {
                metrics.putIfAbsent(entry.getKey(), 0.0);
                metrics.put(entry.getKey(), metrics.get(entry.getKey()) + entry.getValue());
            }
        }

        log.info("Aggregated metrics from queries {} was {}. Repartitioning.",
                SafeArg.of("metricsMaps", metricsValues),
                SafeArg.of("consolidatedMap", metrics));

        return weightedPartition(clients, hosts, seed, metrics);
    }

    @Override
    public Assignment weightedPartition(List<String> clients, List<String> hosts, long seed,
            Map<String, Double> clientToWeight) {
        return delegate.weightedPartition(clients, hosts, seed, clientToWeight);
    }
}
