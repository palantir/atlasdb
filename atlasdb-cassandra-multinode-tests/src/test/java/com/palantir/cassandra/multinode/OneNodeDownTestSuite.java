/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.cassandra.multinode;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;
import com.palantir.atlasdb.keyvalue.cassandra.SchemaMutationLockReleasingRule;
import com.palantir.flake.ShouldRetry;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        OneNodeDownGetTest.class,
        OneNodeDownPutTest.class,
        OneNodeDownMetadataTest.class,
        OneNodeDownDeleteTest.class,
        OneNodeDownTableManipulationTest.class,
        OneNodeDownNodeAvailabilityTest.class
        })
@ShouldRetry(numAttempts = 3)
public final class OneNodeDownTestSuite extends NodesDownTestSetup {

    // Reusing containers when the FlakeRule kicks in.
    @ClassRule
    public static final RuleChain ruleChain = RuleChain
            .outerRule(new Containers(NodesDownTestSetup.class)
                    .with(new ThreeNodeCassandraCluster()))
            .around(SchemaMutationLockReleasingRule.createChainedReleaseAndRetry(
                    createCassandraKvs(), CONFIG));

    @BeforeClass
    public static void setup() {
        NodesDownTestSetup.initializeKvsAndDegradeCluster(
                ImmutableList.of(ThreeNodeCassandraCluster.FIRST_CASSANDRA_CONTAINER_NAME));
    }
}
