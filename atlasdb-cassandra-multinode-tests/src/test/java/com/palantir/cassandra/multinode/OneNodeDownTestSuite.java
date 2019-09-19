/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.cassandra.multinode;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;
import java.util.Arrays;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

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
