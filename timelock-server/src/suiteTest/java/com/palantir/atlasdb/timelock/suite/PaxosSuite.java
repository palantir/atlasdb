/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.suite;

import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import com.github.peterwippermann.junit4.parameterizedsuite.ParameterizedSuite;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.ImmutableTemplateVariables;
import com.palantir.atlasdb.timelock.MultiNodePaxosTimeLockServerIntegrationTest;
import com.palantir.atlasdb.timelock.TemplateVariables;
import com.palantir.atlasdb.timelock.TestableTimelockCluster;

@RunWith(ParameterizedSuite.class)
@Suite.SuiteClasses(MultiNodePaxosTimeLockServerIntegrationTest.class)
public class PaxosSuite {

    public static final TestableTimelockCluster NON_BATCHED_TIMESTAMP_PAXOS = new TestableTimelockCluster(
            "non-batched paxos",
            "paxosMultiServer.ftl",
            generateThreeNodeTimelockCluster(9080, builder ->
                    builder.clientPaxosBuilder(
                            builder.clientPaxosBuilder().isUseBatchPaxos(false))));

    public static final TestableTimelockCluster BATCHED_TIMESTAMP_PAXOS = new TestableTimelockCluster(
            "batched paxos",
            "paxosMultiServer.ftl",
            generateThreeNodeTimelockCluster(9083, builder ->
                    builder.clientPaxosBuilder(
                            builder.clientPaxosBuilder().isUseBatchPaxos(true))));

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestableTimelockCluster> params() {
        return ImmutableSet.of(NON_BATCHED_TIMESTAMP_PAXOS, BATCHED_TIMESTAMP_PAXOS);
    }

    @Rule
    @Parameterized.Parameter
    public TestableTimelockCluster cluster;

    private static Iterable<TemplateVariables> generateThreeNodeTimelockCluster(
            int startingPort,
            UnaryOperator<ImmutableTemplateVariables.Builder> customizer) {
        List<Integer> allPorts = IntStream.range(startingPort, startingPort + 3).boxed().collect(Collectors.toList());
        return IntStream.range(startingPort, startingPort + 3)
                .boxed()
                .map(port -> ImmutableTemplateVariables.builder()
                        .serverPorts(allPorts)
                        .localServerPort(port))
                .map(customizer)
                .map(ImmutableTemplateVariables.Builder::build)
                .collect(Collectors.toList());
    }
}
