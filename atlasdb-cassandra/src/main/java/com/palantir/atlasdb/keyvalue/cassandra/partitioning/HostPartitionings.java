/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.partitioning;

import com.palantir.common.streams.KeyedStream;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

public class HostPartitionings {
    private HostPartitionings() {
        // lol
    }

    public static void main(String[] args) {
        partitionHosts(8, 125);
    }

    // assume rf 3
    public static void partitionHosts(int numHostsPerAz, int numShards) {
        List<IdealHostPartition> idealHostPartitions = MathUtils.partitionForIdealDistribution(numHostsPerAz, numShards);
        ProposedSolution proposedSolution = new ProposedSolution(numShards);

        idealHostPartitions.forEach(partition -> {
            proposedSolution.addTrivialDistribution(MathUtils.generateTokenRingSlices(partition));
        });

        Map<CassandraHost, Integer> numShardsAffectedByHost = proposedSolution.getHostsAssignedToShards().entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().map(host -> Map.entry(host, entry.getKey())))
                .collect(Collectors.groupingBy(
                        Entry::getKey, Collectors.collectingAndThen(Collectors.toList(), list -> list.stream()
                                .map(Entry::getValue)
                                .collect(Collectors.toSet())
                                .size())));
        printOutcome(numShards, proposedSolution.getProposedSolution(), numShardsAffectedByHost);
    }

    private static void printOutcome(
            int numShards,
            Map<SweepShard, List<TokenRingSlice>> proposedSolution,
            Map<CassandraHost, Integer> hostToShards) {

        Optional<Integer> maxShardsKilled = hostToShards.values().stream().max(Comparator.naturalOrder());
        Optional<Integer> minShardsKilled = hostToShards.values().stream().min(Comparator.naturalOrder());

        // all permutations accepted by a shard
        Map<SweepShard, Integer> shardToNumAcceptedPermutations =
                KeyedStream.stream(proposedSolution).map(List::size).collectToMap();

        int minPermutations = shardToNumAcceptedPermutations.values().stream()
                .min(Comparator.naturalOrder())
                .get();
        int maxPermutations = shardToNumAcceptedPermutations.values().stream()
                .max(Comparator.naturalOrder())
                .get();

        List<String> distribution = hostToShards.values()
                .stream()
                .sorted()
                .map(val -> ((double)val / numShards) * 100 + "%")
                .collect(Collectors.toList());

        System.out.println("Distribution of shards killed by loss of host " + distribution);
        System.out.println("Max number of shards killed by loss of host " + maxShardsKilled.get() + ", " + ((double)maxShardsKilled.get() / numShards) * 100 + "%");
        System.out.println("Min number of shards killed by loss of host " + minShardsKilled.get() + ", " + ((double)minShardsKilled.get() / numShards) * 100 + "%");
        System.out.println("Min assigned permutations " + minPermutations);
        System.out.println("Max assigned permutations " + maxPermutations);
        System.out.println();
    }

    private static boolean isBadDistribution(
            int numShards, Optional<Integer> maxShardsKilled, int minPermutations, int maxPermutations) {
        return maxShardsKilled
                        .map(shardsKilled -> shardsKilled > numShards * 2 / 3)
                        .orElse(true)
                || (maxPermutations > 3 * minPermutations);
    }
}
