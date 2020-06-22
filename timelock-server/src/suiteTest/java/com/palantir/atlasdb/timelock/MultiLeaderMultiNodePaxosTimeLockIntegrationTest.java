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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.timelock.suite.MultiLeaderPaxosSuite;
import com.palantir.atlasdb.timelock.util.ParameterInjector;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.errors.QosException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MultiLeaderMultiNodePaxosTimeLockIntegrationTest {

    @ClassRule
    public static ParameterInjector<TestableTimelockCluster> injector =
            ParameterInjector.withFallBackConfiguration(() -> MultiLeaderPaxosSuite.MULTI_LEADER_PAXOS);

    @Parameterized.Parameter
    public TestableTimelockCluster cluster;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestableTimelockCluster> params() {
        return injector.getParameter();
    }


    @Test
    public void eachNamespaceGetsAssignedDifferentLeaders() {
        // the reason this test works is that unless a namespace has been seen by a node, it won't try to gain
        // leadership on that namespace
        // by putting timelock behind a wiremock proxy, we can disallow requests to namespaces effectively giving us
        // the ability to allocate namespaces to particular leaders
        SetMultimap<TestableTimelockServer, String> allocations = allocateRandomNamespacesToAllNodes(3);

        Set<String> allNamespaces = ImmutableSet.copyOf(allocations.values());

        allNamespaces.stream()
                .map(cluster::client)
                .map(NamespacedClients::throughWireMockProxy)
                .forEach(NamespacedClients::getFreshTimestamp);

        SetMultimap<TestableTimelockServer, String> namespacesByLeader =
                ImmutableSetMultimap.copyOf(cluster.currentLeaders(allNamespaces)).inverse();

        assertThat(namespacesByLeader)
                .isEqualTo(allocations);

        cluster.servers().forEach(TestableTimelockServer::allowAllNamespaces);
    }

    @Test
    public void nonLeadersCorrectly308() {
        SetMultimap<TestableTimelockServer, String> allocations = allocateRandomNamespacesToAllNodes(1);
        String randomNamespace = Iterables.get(allocations.values(), 0);
        NamespacedClients clientForRandomNamespace = cluster.client(randomNamespace).throughWireMockProxy();
        clientForRandomNamespace.getFreshTimestamp();

        cluster.servers().forEach(TestableTimelockServer::allowAllNamespaces);

        TestableTimelockServer allocatedLeader = Iterables.getOnlyElement(
                ImmutableSetMultimap.copyOf(allocations).inverse().get(randomNamespace));

        assertThat(cluster.currentLeaderFor(randomNamespace))
                .isEqualTo(allocatedLeader);

        cluster.nonLeaders(randomNamespace).get(randomNamespace).forEach(nonLeader ->
                assertThatThrownBy(() -> nonLeader.client(randomNamespace).getFreshTimestamp())
                        .as("non leaders should return 308")
                        .hasRootCauseInstanceOf(QosException.RetryOther.class));
    }

    private SetMultimap<TestableTimelockServer, String> allocateRandomNamespacesToAllNodes(int namespacesPerLeader) {
        SetMultimap<TestableTimelockServer, String> namespaceAllocations = KeyedStream.of(cluster.servers())
                .flatMap(unused -> Stream.generate(MultiLeaderMultiNodePaxosTimeLockIntegrationTest::randomNamespace)
                        .limit(namespacesPerLeader))
                .collectToSetMultimap();

        Multimaps.asMap(namespaceAllocations).forEach(TestableTimelockServer::rejectAllNamespacesOtherThan);

        return namespaceAllocations;
    }

    private static String randomNamespace() {
        return UUID.randomUUID().toString();
    }

}
