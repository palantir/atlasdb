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

import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.logsafe.Preconditions;
import java.util.List;
import java.util.Map;

/**
 * Algorithm only works for RF = 3
 * */
public class PartitioningAlgorithm {

    public static ProposedSolutionBuilder partitionHosts(
            Map<String, List<CassandraServer>> availabilityZoneToHosts, int numShards) {
        Preconditions.checkState(
                availabilityZoneToHosts.keySet().size() == 3, "Only works for replication factor of " + "3");

        List<IdealHostPartition> idealHostPartitions =
                Utils.partitionForIdealDistribution(availabilityZoneToHosts, numShards);
        ProposedSolutionBuilder proposedSolution = new ProposedSolutionBuilder(numShards);

        idealHostPartitions.forEach(partition -> {
            proposedSolution.addTrivialDistribution(Utils.generateTokenRangeEndpointDetailss(partition));
        });

        return proposedSolution;
    }
}
