/**
 * Copyright 2016 Palantir Technologies
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

import org.junit.ClassRule;

import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;
import com.palantir.docker.compose.connection.DockerMachine;

public class CassandraSchemaLockTest37 extends CassandraSchemaLockTest {
    private static String cassandraVersion = "3.7";

    private static DockerMachine dockerMachine = DockerMachine.localMachine()
            .withAdditionalEnvironmentVariable("CASSANDRA_VERSION", cassandraVersion)
            .build();

    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraSchemaLockTest37.class, dockerMachine)
            .with(new ThreeNodeCassandraCluster(cassandraVersion));

    @Override
    protected Containers getContainers() {
        return CONTAINERS;
    }
}
