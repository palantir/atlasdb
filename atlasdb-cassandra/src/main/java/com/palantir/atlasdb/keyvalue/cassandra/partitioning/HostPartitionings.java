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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HostPartitionings {
    private static final Random RANDOM = new Random(0);
    private static final int ITERATIONS = 1000;
    private static final int RF = 3;

    private HostPartitionings() {
        // lol
    }

    public static void main(String[] args) {
        partitionHosts(24, 64);
    }

    // assume rf 3
    public static void partitionHosts(int numHosts, int numShards) {
        List<List<CassandraHost>> cassHostPerms = generateCassandraHostPermutations(numHosts);

        for (int i = 0; i < ITERATIONS; i++) {
            calculateDistribution(numHosts, numShards, cassHostPerms);
        }
    }

    private static void calculateDistribution(int numHosts, int numShards, List<List<CassandraHost>> hostPermutations) {

        int halfHosts = numHosts / 2;
        int halfShards = numShards / 2;
        int numberOfPartitionAbidingShards = RANDOM.nextInt(numShards - 1 - halfShards) + halfShards;
        int numberOfHostsBlocklistedByPartitionAbidingShards = RANDOM.nextInt(numHosts - 3 - halfHosts) + halfHosts;

        List<SweepShard> partitionAbidingShards = generatepartitionAbidingShards(numberOfPartitionAbidingShards);

        Map<SweepShard, Set<CassandraHost>> blocklistedHosts = KeyedStream.of(partitionAbidingShards)
                .map(crap -> Utils.getSetOfBlockListedHosts(numHosts, numberOfHostsBlocklistedByPartitionAbidingShards))
                .collectToMap();

        Map<SweepShard, List<List<CassandraHost>>> proposedSolution = new HashMap<>();

        IntStream.range(0, numShards)
                .forEach(shard -> proposedSolution.put(ImmutableSweepShard.of(shard), new ArrayList<>()));

        for (List<CassandraHost> permutation : hostPermutations) {
            assignPermutationToShard(blocklistedHosts, proposedSolution, permutation);
        }

        Map<SweepShard, Set<CassandraHost>> hostsAssignedToShards = KeyedStream.stream(proposedSolution)
                .map(list -> list.stream().flatMap(List::stream).collect(Collectors.toSet()))
                .collectToMap();

        Map<CassandraHost, Integer> numShardsAffectedByHost = hostsAssignedToShards.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().map(host -> Map.entry(host, entry.getKey())))
                .collect(Collectors.groupingBy(
                        Entry::getKey, Collectors.collectingAndThen(Collectors.toList(), list -> list.stream()
                                .map(Entry::getValue)
                                .collect(Collectors.toSet())
                                .size())));
        printOutcome(
                numShards,
                numberOfPartitionAbidingShards,
                numberOfHostsBlocklistedByPartitionAbidingShards,
                proposedSolution,
                numShardsAffectedByHost);
    }

    private static void assignPermutationToShard(
            Map<SweepShard, Set<CassandraHost>> blocklistedHosts,
            Map<SweepShard, List<List<CassandraHost>>> proposedSolution,
            List<CassandraHost> permutation) {
        List<Map.Entry<Integer, SweepShard>> assignedPermutationsCount = proposedSolution.entrySet().stream()
                .map(entry -> Map.entry(entry.getValue().size(), entry.getKey()))
                .collect(Collectors.toList());

        assignedPermutationsCount.sort(
                Comparator.comparing((Function<Entry<Integer, SweepShard>, Integer>) Entry::getKey)
                        .thenComparing(e -> e.getValue().id()));

        SweepShard nextViableShard = assignedPermutationsCount.stream()
                .filter(shard -> Utils.canAddPermutation(
                        blocklistedHosts, permutation, shard.getValue().id()))
                .findFirst()
                .get()
                .getValue();

        proposedSolution
                .computeIfAbsent(nextViableShard, _ignore -> new ArrayList<>())
                .add(permutation);
    }

    private static List<SweepShard> generatepartitionAbidingShards(int numberOfPartitionAbidingShards) {
        return IntStream.range(0, numberOfPartitionAbidingShards)
                .boxed()
                .map(ImmutableSweepShard::of)
                .collect(Collectors.toList());
    }

    private static void printOutcome(
            int numShards,
            int numberOfNonJunkShards,
            int numberOfBlocklistedHostsInNonJunkShards,
            Map<SweepShard, List<List<CassandraHost>>> proposedSolution,
            Map<CassandraHost, Integer> hostToShards) {

        Optional<Integer> maxShardsKilled = hostToShards.values().stream().max(Comparator.naturalOrder());

        // all permutations accepted by a shard
        Map<SweepShard, Integer> shardToNumAcceptedPermutations =
                KeyedStream.stream(proposedSolution).map(List::size).collectToMap();

        int minPermutations = shardToNumAcceptedPermutations.values().stream()
                .min(Comparator.naturalOrder())
                .get();
        int maxPermutations = shardToNumAcceptedPermutations.values().stream()
                .max(Comparator.naturalOrder())
                .get();

        if (isBadDistribution(numShards, maxShardsKilled, minPermutations, maxPermutations)) {
            return;
        }

        // Print out acceptable distributions
        System.out.println("Nonjunk shards " + numberOfNonJunkShards);
        System.out.println("Blocklisted hosts per shard " + numberOfBlocklistedHostsInNonJunkShards);
        System.out.println("Max number of shards killed by loss of host " + maxShardsKilled);
        System.out.println("Min assigned permutations" + minPermutations);
        System.out.println("Max assigned permutations" + maxPermutations);
        System.out.println();
    }

    private static boolean isBadDistribution(
            int numShards, Optional<Integer> maxShardsKilled, int minPermutations, int maxPermutations) {
        return maxShardsKilled
                        .map(shardsKilled -> shardsKilled > numShards * 2 / 3)
                        .orElse(true)
                || (maxPermutations > 3 * minPermutations);
    }

    private static List<List<CassandraHost>> generateCassandraHostPermutations(int numHosts) {
        List<List<Integer>> hostPermutations = PermutationGenerator.generatePartitions(RF, numHosts);
        List<List<CassandraHost>> cassHostPerms = hostPermutations.stream()
                .map(perm -> {
                    List<CassandraHost> hosts =
                            perm.stream().map(ImmutableCassandraHost::of).collect(Collectors.toList());
                    return hosts;
                })
                .collect(Collectors.toList());
        return cassHostPerms;
    }
}
