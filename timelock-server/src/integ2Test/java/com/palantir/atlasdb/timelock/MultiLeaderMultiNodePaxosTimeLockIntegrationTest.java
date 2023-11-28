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

package com.palantir.atlasdb.timelock;

import static com.palantir.atlasdb.timelock.TemplateVariables.generateThreeNodeTimelockCluster;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.timelock.util.TestableTimeLockClusterPorts;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class MultiLeaderMultiNodePaxosTimeLockIntegrationTest {

    @RegisterExtension
    public static final TestableTimelockClusterV2 cluster = new TestableTimelockClusterV2(
            "batched timestamp paxos multi leader",
            "paxosMultiServer.ftl",
            generateThreeNodeTimelockCluster(
                    TestableTimeLockClusterPorts.MULTI_LEADER_PAXOS_SUITE, builder -> builder.clientPaxosBuilder(
                                    builder.clientPaxosBuilder().isUseBatchPaxosTimestamp(true))
                            .leaderMode(PaxosLeaderMode.LEADER_PER_CLIENT)));

    @Test
    public void eachNamespaceGetsAssignedDifferentLeaders() {
        // the reason this test works is that unless a namespace has been seen by a node, it won't try to gain
        // leadership on that namespace
        // by putting timelock behind a wiremock proxy, we can disallow requests to namespaces effectively giving us
        // the ability to allocate namespaces to particular leaders
        SetMultimap<TestableTimelockServerV2, String> allocations = allocateRandomNamespacesToAllNodes(3);

        Set<String> allNamespaces = ImmutableSet.copyOf(allocations.values());

        allNamespaces.stream()
                .map(cluster::client)
                .map(NamespacedClientsV2::throughWireMockProxy)
                .forEach(NamespacedClientsV2::getFreshTimestamp);

        SetMultimap<TestableTimelockServerV2, String> namespacesByLeader = ImmutableSetMultimap.copyOf(
                        cluster.currentLeaders(allNamespaces))
                .inverse();

        assertThat(namespacesByLeader).isEqualTo(allocations);

        cluster.servers().forEach(TestableTimelockServerV2::allowAllNamespaces);
    }

    @Test
    public void nonLeadersCorrectly308() {
        SetMultimap<TestableTimelockServerV2, String> allocations = allocateRandomNamespacesToAllNodes(1);
        String randomNamespace = Iterables.get(allocations.values(), 0);
        NamespacedClientsV2 clientForRandomNamespace =
                cluster.client(randomNamespace).throughWireMockProxy();
        clientForRandomNamespace.getFreshTimestamp();

        cluster.servers().forEach(TestableTimelockServerV2::allowAllNamespaces);

        TestableTimelockServerV2 allocatedLeader = Iterables.getOnlyElement(
                ImmutableSetMultimap.copyOf(allocations).inverse().get(randomNamespace));

        assertThat(cluster.currentLeaderFor(randomNamespace)).isEqualTo(allocatedLeader);

        cluster.nonLeaders(randomNamespace).get(randomNamespace).forEach(nonLeader -> assertThatThrownBy(
                        () -> nonLeader.client(randomNamespace).getFreshTimestamp())
                .as("non leaders should return 308")
                .hasRootCauseInstanceOf(QosException.RetryOther.class));
    }

    private SetMultimap<TestableTimelockServerV2, String> allocateRandomNamespacesToAllNodes(int namespacesPerLeader) {
        SetMultimap<TestableTimelockServerV2, String> namespaceAllocations = KeyedStream.of(cluster.servers())
                .flatMap(unused -> Stream.generate(MultiLeaderMultiNodePaxosTimeLockIntegrationTest::randomNamespace)
                        .limit(namespacesPerLeader))
                .collectToSetMultimap();

        Multimaps.asMap(namespaceAllocations).forEach(TestableTimelockServerV2::rejectAllNamespacesOtherThan);

        return namespaceAllocations;
    }

    private static String randomNamespace() {
        return UUID.randomUUID().toString();
    }
}
