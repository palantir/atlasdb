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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Throwables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.ClassRule;
import org.junit.Test;

public class CassandraSchemaLockTest {
    private static final int THREAD_COUNT = 4;

    @ClassRule
    public static final Containers CONTAINERS =
            new Containers(CassandraSchemaLockTest.class).with(new ThreeNodeCassandraCluster());

    private final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

    @Test
    public void shouldCreateTablesConsistentlyWithMultipleCassandraNodes() throws Exception {
        TableReference table1 = TableReference.createFromFullyQualifiedName("ns.table1");
        CassandraKeyValueServiceConfig config = ThreeNodeCassandraCluster.KVS_CONFIG;

        try {
            CyclicBarrier barrier = new CyclicBarrier(THREAD_COUNT);
            for (int i = 0; i < THREAD_COUNT; i++) {
                async(() -> {
                    CassandraKeyValueService keyValueService = CassandraKeyValueServiceImpl.createForTesting(config);
                    barrier.await();
                    keyValueService.createTable(table1, AtlasDbConstants.GENERIC_TABLE_METADATA);
                    return null;
                });
            }
        } finally {
            executorService.shutdown();
            assertThat(executorService.awaitTermination(4, TimeUnit.MINUTES)).isTrue();
        }

        CassandraKeyValueService kvs = CassandraKeyValueServiceImpl.createForTesting(config);
        assertThat(kvs.getAllTableNames()).contains(table1);

        assertThat(new File(CONTAINERS.getLogDirectory()).listFiles()).allSatisfy(file -> {
            Path path = Paths.get(file.getAbsolutePath());
            try (Stream<String> lines = Files.lines(path, StandardCharsets.ISO_8859_1)) {
                List<String> badLines = lines.filter(line -> line.contains("Column family ID mismatch"))
                        .collect(Collectors.toList());
                assertThat(badLines)
                        .describedAs("File called %s which contains lines %s", file.getAbsolutePath(), badLines)
                        .isEmpty();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        });
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private void async(Callable callable) {
        executorService.submit(callable);
    }
}
