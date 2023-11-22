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

import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.ExtendWith;

@NodesDownTestClass
@Order(6) // Take the third node down and continue.
@ExtendWith(NodesDownTestSetup.class)
public class ThreeNodesDownAvailabilityTest extends AbstractNodeAvailabilityTest {

    @BeforeAll
    public static void beforeAll() {
        NodesDownTestSetup.degradeCassandraCluster(ThreeNodeCassandraCluster.THIRD_CASSANDRA_CONTAINER_NAME);
    }

    @Override
    protected ClusterAvailabilityStatus expectedNodeAvailabilityStatus() {
        return ClusterAvailabilityStatus.NO_QUORUM_AVAILABLE;
    }
}
