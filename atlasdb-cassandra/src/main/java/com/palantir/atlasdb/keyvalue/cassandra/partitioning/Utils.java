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
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class Utils {
    public static List<TokenRangeEndpoints> generateTokenRangeEndpointDetailss(IdealHostPartition idealHostPartition) {
        List<TokenRangeEndpoints> result = new ArrayList<>();

        for (CassandraServer hostA : idealHostPartition.setA()) {
            for (CassandraServer hostB : idealHostPartition.setB()) {
                for (CassandraServer hostC : idealHostPartition.setC()) {
                    result.add(ImmutableTokenRangeEndpointDetails.builder()
                            .addHosts(hostA, hostB, hostC)
                            .build());
                }
            }
        }

        return result;
    }

    public static List<IdealHostPartition> partitionForIdealDistribution(
            Map<String, List<CassandraServer>> availabilityZoneToHosts, int shards) {
        // todo(snanda): cube root as we are assuming RF = 3
        int elemsPerSetForIdealDistribution = (int) Math.ceil(Math.cbrt(shards));
        List<IdealHostPartition> result = new ArrayList<>();

        List<List<CassandraServer>> hostListsPerAz = (List<List<CassandraServer>>) availabilityZoneToHosts.values();

        int numHostsPerAz = hostListsPerAz.get(0).size();
        for (int i = 0; i < numHostsPerAz; i += elemsPerSetForIdealDistribution) {
            for (int j = 0; j < numHostsPerAz; j += elemsPerSetForIdealDistribution) {
                for (int k = 0; k < numHostsPerAz; k += elemsPerSetForIdealDistribution) {
                    List<CassandraServer> setA =
                            getIdealHostsSubset(i, elemsPerSetForIdealDistribution, hostListsPerAz.get(0));
                    List<CassandraServer> setB =
                            getIdealHostsSubset(j, elemsPerSetForIdealDistribution, hostListsPerAz.get(1));
                    List<CassandraServer> setC =
                            getIdealHostsSubset(k, elemsPerSetForIdealDistribution, hostListsPerAz.get(2));

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

        int remainderHosts = numHostsPerAz % elemsPerSetForIdealDistribution;

        IdealHostPartition idealHostPartition = ImmutableIdealHostPartition.builder()
                .addAllSetA(getIdealHostsSubset(numHostsPerAz - remainderHosts, remainderHosts, hostListsPerAz.get(0)))
                .addAllSetB(getIdealHostsSubset(numHostsPerAz - remainderHosts, remainderHosts, hostListsPerAz.get(1)))
                .addAllSetC(getIdealHostsSubset(numHostsPerAz - remainderHosts, remainderHosts, hostListsPerAz.get(2)))
                .build();
        result.add(idealHostPartition);

        return result;
    }

    private static List<CassandraServer> getIdealHostsSubset(
            int index, int elemsPerSetForIdealDistribution, List<CassandraServer> cassandraServers) {
        if (index + elemsPerSetForIdealDistribution > cassandraServers.size()) {
            return ImmutableList.of();
        }
        return cassandraServers.subList(index, index + elemsPerSetForIdealDistribution);
    }
}
