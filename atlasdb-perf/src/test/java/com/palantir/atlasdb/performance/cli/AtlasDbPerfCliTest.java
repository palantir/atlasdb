/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.performance.cli;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.performance.backend.DatabasesContainer;
import com.palantir.atlasdb.performance.backend.KeyValueServiceInstrumentation;

@RunWith(Parameterized.class)
public class AtlasDbPerfCliTest {
    public static List<String> excluded = ImmutableList.<String>builder()
            .add("KvsGetRowsColumnRangeBenchmarks.getAllColumnsUnaligned")
            .add("KvsGetRangeBenchmarks.getSingleRange")
            .add("TransactionGetBenchmarks.getSingleCell")
            .add("KvsGetRangeBenchmarks.getMultiRangeDirty")
            .add("KvsGetRangeBenchmarks.getSingleLargeRange")
            .add("TransactionGetBenchmarks.getRange")
            .build();

    @Parameterized.Parameter
    public String benchmark;

    @Parameterized.Parameters
    public static Collection<String> benchmarks() {
        Set<String> list = AtlasDbPerfCli.getAllBenchmarks();
        excluded.forEach(list::remove);
        return list;
    }

    private static Map<KeyValueServiceInstrumentation, String> dockerMap;
    private static DatabasesContainer docker;

//    @BeforeClass
//    public static void setup() {
//        Set<String> backends = KeyValueServiceInstrumentation.getBackends();
//        docker = AtlasDbPerfCli.startupDatabase(backends);
//        dockerMap = docker.getDockerizedDatabases().stream().map(
//                DockerizedDatabase::getUri).collect(
//                Collectors.toMap(DockerizedDatabaseUri::getKeyValueServiceInstrumentation,
//                        DockerizedDatabaseUri::toString));
//    }

    @Test
    public void postgresSingleIteration() throws Exception {
        String[] args = {"--db-uri", "CASSANDRA@127.0.0.1:9160",
                         //dockerMap.get(KeyValueServiceInstrumentation.forDatabase("POSTGRES")),
                         "--test-run",
                         "--benchmark", benchmark};
        AtlasDbPerfCli.main(args);
    }

//    @Test
//    public void cassandraSingleIteration() throws Exception {
//        String[] args = {"--db-uri", dockerMap.get(KeyValueServiceInstrumentation.forDatabase("CASSANDRA")),
//                         "--test-run",
//                         "--benchmark", benchmark};
//        AtlasDbPerfCli.main(args);
//    }

    @AfterClass
    public static void close() throws Exception {
//        docker.close();
    }
}
