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

package com.palantir.atlasdb.timelock;

import java.util.Collection;

import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import com.github.peterwippermann.junit4.parameterizedsuite.ParameterizedSuite;
import com.google.common.collect.ImmutableSet;

@RunWith(ParameterizedSuite.class)
@Suite.SuiteClasses({MultiNodePaxosTimeLockServerIntegrationTest.class})
public class PaxosSuite {

    static final TestableTimelockCluster NON_BATCHED_PAXOS = new TestableTimelockCluster(
            "https://localhost",
            "paxosMultiServer0.yml",
            "paxosMultiServer1.yml",
            "paxosMultiServer2.yml");

    static final TestableTimelockCluster BATCHED_PAXOS = new TestableTimelockCluster(
            "https://localhost",
            "paxosMultiServerBatch0.yml",
            "paxosMultiServerBatch1.yml",
            "paxosMultiServerBatch2.yml");

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestableTimelockCluster> params() {
        return ImmutableSet.of(NON_BATCHED_PAXOS, BATCHED_PAXOS);
    }

    @Rule
    @Parameterized.Parameter
    public TestableTimelockCluster cluster;
}
