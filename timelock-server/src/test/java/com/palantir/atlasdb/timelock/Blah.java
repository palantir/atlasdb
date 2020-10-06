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

import java.util.Collection;

import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import com.github.peterwippermann.junit4.parameterizedsuite.ParameterizedSuite;
import com.google.common.collect.ImmutableSet;
import com.palantir.timelock.config.PaxosInstallConfiguration;

@RunWith(ParameterizedSuite.class)
@Suite.SuiteClasses({
        MultiNodePaxosTimeLockServerIntegrationTest.class,
        SingleLeaderMultiNodePaxosTimeLockIntegrationTest.class
})
public class Blah {
    public static final TestableTimelockCluster DB_TIMELOCK = new TestableTimelockCluster(
            "db TimeLock",
            "dbKvsServer.ftl",
            generateThreeNodeTimelockCluster(9080, builder ->
                    builder.clientPaxosBuilder(builder.clientPaxosBuilder().isUseBatchPaxosTimestamp(false))
                            .leaderMode(PaxosInstallConfiguration.PaxosLeaderMode.SINGLE_LEADER)
                            .dbConfig(BlahSuite.getKvsConfig())));

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestableTimelockCluster> params() {
        return ImmutableSet.of(DB_TIMELOCK);
    }

    @Rule
    @Parameterized.Parameter
    public TestableTimelockCluster cluster;

}
