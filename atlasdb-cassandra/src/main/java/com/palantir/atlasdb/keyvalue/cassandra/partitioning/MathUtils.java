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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

// todo(snanda): only works for exact partitions  | Also only works for RF = 3
public final class MathUtils {

    public static List<TokenRingSlice> generateTokenRingSlices(IdealHostPartition idealHostPartition) {
        List<TokenRingSlice> result = new ArrayList<>();

        for (CassandraHost hostA: idealHostPartition.setA()) {
            for (CassandraHost hostB: idealHostPartition.setB()) {
                for (CassandraHost hostC: idealHostPartition.setC()) {
                    result.add(ImmutableTokenRingSlice.builder().addHosts(hostA, hostB, hostC).build());
                }
            }
        }

        return result;
    }

    public static List<IdealHostPartition> partitionForIdealDistribution(int numHostsperAz, int shards) {
        int elemsPerSetForIdealDistribution = (int) Math.cbrt(shards);
        List<IdealHostPartition> result = new ArrayList<>();

        for (int i = 0; i < numHostsperAz ; i+=elemsPerSetForIdealDistribution) {
            for (int j = 0; j < numHostsperAz; j+=elemsPerSetForIdealDistribution) {
                for (int k = 0; k < numHostsperAz; k+=elemsPerSetForIdealDistribution) {
                    List<CassandraHost> setA = getIdealHostsSubset("a", i, elemsPerSetForIdealDistribution, numHostsperAz);
                    List<CassandraHost> setB = getIdealHostsSubset("b", j, elemsPerSetForIdealDistribution, numHostsperAz);
                    List<CassandraHost> setC = getIdealHostsSubset("c", k, elemsPerSetForIdealDistribution, numHostsperAz);

                    if (setA.size() < elemsPerSetForIdealDistribution
                            || setB.size() < elemsPerSetForIdealDistribution
                                || setC.size() < elemsPerSetForIdealDistribution) {
                        continue;
                    }

                    IdealHostPartition idealHostPartition = ImmutableIdealHostPartition.builder()
                            .addAllSetA(setA)
                            .addAllSetB(setB)
                            .addAllSetC(setC)
                                    .build();
                    result.add(idealHostPartition);
                }
            }
        }

        int remainderHosts = numHostsperAz % elemsPerSetForIdealDistribution;

        IdealHostPartition idealHostPartition = ImmutableIdealHostPartition.builder()
                .addAllSetA(getIdealHostsSubset("a", numHostsperAz - remainderHosts, remainderHosts, numHostsperAz))
                .addAllSetB(getIdealHostsSubset("b", numHostsperAz - remainderHosts, remainderHosts, numHostsperAz))
                .addAllSetC(getIdealHostsSubset("c", numHostsperAz - remainderHosts, remainderHosts, numHostsperAz))
                .build();
        result.add(idealHostPartition);

        return result;
    }

    private static List<CassandraHost> getIdealHostsSubset(String az, int index, int elems, int numHostsperAz) {
        if (index + elems > numHostsperAz) {
            return ImmutableList.of();
        }
        return IntStream.range(index, index + elems).mapToObj(idx -> ImmutableCassandraHost.of(az + idx)).collect(Collectors.toList());
    }
}
