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

package com.palantir.atlasdb.timelock.stress;

import static com.palantir.atlasdb.timelock.TemplateVariables.generateThreeNodeTimelockCluster;

import com.github.peterwippermann.junit4.parameterizedsuite.ParameterizedSuite;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.MultiNodePaxosTimeLockServerStressTest;
import com.palantir.atlasdb.timelock.TemplateVariables;
import com.palantir.atlasdb.timelock.TestableTimelockCluster;
import com.palantir.atlasdb.timelock.TestableTimelockServerConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(ParameterizedSuite.class)
@Suite.SuiteClasses(MultiNodePaxosTimeLockServerStressTest.class)
public class DbTimeLockSingleLeaderPaxosStressTests {
    private static final Iterable<TemplateVariables> TEMPLATE_VARIABLES =
            generateThreeNodeTimelockCluster(9086, builder -> builder.clientPaxosBuilder(builder.clientPaxosBuilder()
                            .isUseBatchPaxosTimestamp(false)
                            .isBatchSingleLeader(true))
                    .leaderMode(PaxosInstallConfiguration.PaxosLeaderMode.SINGLE_LEADER));
    private static final List<TestableTimelockServerConfiguration> TESTABLE_CONFIGURATIONS = StreamSupport.stream(
                    TEMPLATE_VARIABLES.spliterator(), false)
            .map(variables -> TestableTimelockServerConfiguration.of(variables, true))
            .collect(Collectors.toList());

    public static final TestableTimelockCluster DB_TIMELOCK_CLUSTER = new TestableTimelockCluster(
            "db-timelock; batched single leader", "dbTimeLockPaxosMultiServer.ftl", TESTABLE_CONFIGURATIONS);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestableTimelockCluster> params() {
        return ImmutableSet.of(DB_TIMELOCK_CLUSTER);
    }

    @Rule
    @Parameterized.Parameter
    public TestableTimelockCluster cluster;
}
