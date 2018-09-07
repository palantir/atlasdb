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

import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        OneNodeDownGetTest.class,
        OneNodeDownPutTest.class,
        OneNodeDownMetadataTest.class,
        OneNodeDownDeleteTest.class,
        OneNodeDownTableManipulationTest.class,
        OneNodeDownNodeAvailabilityTest.class
        })
public final class OneNodeDownTestSuite extends NodesDownTestSetup {

    @BeforeClass
    public static void setup() throws Exception {
        initializeKvsAndDegradeCluster(
                Arrays.asList(OneNodeDownTestSuite.class.getAnnotation(Suite.SuiteClasses.class).value()),
                ImmutableList.of(ThreeNodeCassandraCluster.FIRST_CASSANDRA_CONTAINER_NAME));
    }
}
