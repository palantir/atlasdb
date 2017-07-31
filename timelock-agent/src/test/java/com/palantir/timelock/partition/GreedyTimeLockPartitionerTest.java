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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

public class GreedyTimeLockPartitionerTest {
    private static final GreedyTimeLockPartitioner THREE_NODE_MINICLUSTER_PARTITIONER =
            new GreedyTimeLockPartitioner(3);

    @Test
    public void partitionsSixClientsInThreeNodeClusterWithMiniclusterSizeOne() {
        GreedyTimeLockPartitioner partitioner = new GreedyTimeLockPartitioner(1);
        Assignment assignment = invokePartitioner(partitioner, 6, 3);
        assertThat(getNumClientsOnHosts(assignment)).containsExactlyInAnyOrder(2, 2, 2);
    }

    @Test
    public void assignsAllClientsToAllNodesInThreeNodeClusterWithMiniclusterSizeThree() {
        int numClients = 4242;
        Assignment assignment = invokePartitioner(THREE_NODE_MINICLUSTER_PARTITIONER, numClients, 3);
        assertThat(getNumClientsOnHosts(assignment)).containsExactlyInAnyOrder(numClients, numClients, numClients);
    }

    @Test
    public void assignsClientToOnlyThreeNodesInFiveNodeClusterWithMiniclusterSizeThree() {
        Assignment assignment = invokePartitioner(THREE_NODE_MINICLUSTER_PARTITIONER, 1, 5);
        assertThat(getNumClientsOnHosts(assignment)).containsExactlyInAnyOrder(1, 1, 1);
    }

    @Test
    public void onlyOneNodeHasTwoClientsInFiveNodeClusterWithTwoClientsAndMiniclusterSizeThree() {
        Assignment assignment = invokePartitioner(THREE_NODE_MINICLUSTER_PARTITIONER, 2, 5);
        assertThat(getNumClientsOnHosts(assignment)).containsExactlyInAnyOrder(2, 1, 1, 1, 1);
    }

    @Test
    public void onlyTwoNodesHaveThreeClientsInFiveNodeClusterWithFourClientsAndMiniclusterSizeThree() {
        Assignment assignment = invokePartitioner(THREE_NODE_MINICLUSTER_PARTITIONER, 4, 5);
        assertThat(getNumClientsOnHosts(assignment)).containsExactlyInAnyOrder(3, 3, 2, 2, 2);
    }

    private static Assignment invokePartitioner(
            TimeLockPartitioner partitioner,
            int numClients,
            int numHosts) {
        return partitioner.partition(
                IntStream.rangeClosed(1, numClients).mapToObj(Integer::toString).collect(Collectors.toList()),
                IntStream.rangeClosed(1, numHosts).mapToObj(Integer::toString).collect(Collectors.toList()),
                0L);
    }

    private static List<Integer> getNumClientsOnHosts(Assignment assignment) {
        return assignment.getKnownHosts()
                .stream()
                .map(host -> assignment.getClientsForHost(host).size())
                .collect(Collectors.toList());
    }
}
