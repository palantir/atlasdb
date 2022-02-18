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

import com.google.common.collect.Multimap;
import com.palantir.common.streams.KeyedStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HostPartitionings {
    private static final Random RANDOM = new Random(0);

    private HostPartitionings() {
        // lol
    }

    public static void main(String[] args) {
        assignHostsToShards(32, 32);
    }

    // assume rf 3
    public static Multimap<Integer, Integer> assignHostsToShards(int numHosts, int numShards) {
        Map<Integer, Set<Integer>> blocklist = KeyedStream.of(
                        IntStream.range(0, numShards - 2).boxed())
                .map(_ignore -> IntStream.range(0, 20)
                        .mapToObj(_ingore -> RANDOM.nextInt(numHosts))
                        .collect(Collectors.toSet()))
                .collectToMap();
        List<List<Integer>> hostPermutations = generatePartitions(3, numHosts);
        Map<Integer, List<List<Integer>>> proposedSolution = new HashMap<>();
        for (List<Integer> permutation : hostPermutations) {
            int nextShard = RANDOM.nextInt(numShards);
            while (!tryAdd(blocklist, permutation, nextShard)) {
                nextShard = RANDOM.nextInt(numShards);
            }
            proposedSolution
                    .computeIfAbsent(nextShard, _ignore -> new ArrayList<>())
                    .add(permutation);
        }

        Map<Integer, Set<Integer>> coolStuff = KeyedStream.stream(proposedSolution)
                .map(list -> list.stream().flatMap(List::stream).collect(Collectors.toSet()))
                .collectToMap();
        Map<Integer, Integer> objectStream = coolStuff.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().map(host -> Map.entry(host, entry.getKey())))
                .collect(Collectors.groupingBy(
                        Entry::getKey, Collectors.collectingAndThen(Collectors.toList(), list -> list.stream()
                                .map(Map.Entry::getValue)
                                .collect(Collectors.toSet())
                                .size())));
        System.out.println(objectStream);
        System.out.println(KeyedStream.stream(proposedSolution).map(List::size).collectToMap());
        return null;
    }

    private static boolean tryAdd(Map<Integer, Set<Integer>> blocklist, List<Integer> permutation, int proposedShard) {
        return permutation.stream()
                .noneMatch(num -> blocklist.containsKey(proposedShard)
                        && blocklist.get(proposedShard).contains(num));
    }

    private static List<List<Integer>> generatePartitions(int rf, int hosts) {
        List<List<Integer>> tt = new ArrayList<>();
        tt.add(new ArrayList<>());
        List<List<Integer>> allPerms = partitionGenerator(hosts, rf, 0, tt);
        return allPerms.stream().filter(x -> x.size() == rf).collect(Collectors.toList());
    }

    private static List<List<Integer>> partitionGenerator(int num, int rf, int idx, List<List<Integer>> answer) {
        if (idx == num) {
            return answer;
        }

        int size = answer.size();

        for (int i = 0; i < size; i++) {
            List<Integer> temp = new ArrayList<>(answer.get(i));
            if (temp.size() < rf) {
                temp.add(idx);
                answer.add(temp);
            }
        }

        return partitionGenerator(num, rf, idx + 1, answer);
    }
}
