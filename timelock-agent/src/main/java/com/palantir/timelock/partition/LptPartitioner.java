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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Implements the LPT algorithm:
 * Sort all hosts in reverse order, in terms of their weights.
 * Then begin assignment. Assign each client to the minicluster nodes with the lowest weight.
 */
public class LptPartitioner implements TimeLockPartitioner {
    private final int miniclusterSize;

    public LptPartitioner(int miniclusterSize) {
        this.miniclusterSize = miniclusterSize;
    }

    @Override
    public Assignment partition(List<String> clients, List<String> hosts, long seed) {
        return weightedPartition(clients, hosts, seed,
                clients.stream().collect(Collectors.toMap(client -> client, unused -> 1.0)));
    }

    @Override
    public Assignment weightedPartition(List<String> clients, List<String> hosts, long seed,
            Map<String, Double> clientToWeight) {
        Preconditions.checkArgument(hosts.size() >= miniclusterSize,
                "Cannot partition hosts into miniclusters larger than the number of hosts.");
        Assignment.Builder assignmentBuilder = Assignment.builder();

        PriorityQueue<HostAndWeight> queue = new PriorityQueue<>(Comparator.comparingDouble(HostAndWeight::weight));

        List<String> clientsInDescendingWeight = clientToWeight.entrySet().stream()
                .sorted((entry1, entry2) -> {
                    int delta = Double.compare(entry1.getValue(), entry2.getValue());
                    if (delta != 0) {
                        return delta;
                    }
                    return entry1.getKey().compareTo(entry2.getKey());
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        hosts.stream()
                .map(host -> ImmutableHostAndWeight.of(host, 0.0))
                .forEach(queue::add);

        for (String client : clientsInDescendingWeight) {
            double clientWeight = clientToWeight.get(client);

            // Pull minicluster many nodes
            Set<HostAndWeight> hostsToAssign = Sets.newHashSet();
            for (int i = 0; i < miniclusterSize; i++) {
                hostsToAssign.add(queue.poll());
            }

            hostsToAssign.stream()
                    .map(HostAndWeight::host)
                    .forEach(host -> assignmentBuilder.addMapping(client, host));
            hostsToAssign.forEach(hostAndWeight ->
                    queue.add(ImmutableHostAndWeight.of(hostAndWeight.host(), hostAndWeight.weight() + clientWeight)));
        }

        return assignmentBuilder.build();
    }

    @Value.Immutable
    interface HostAndWeight {
        @Value.Parameter
        String host();

        @Value.Parameter
        double weight();
    }
}
