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

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CassandraSchemaLockTest {
    private static final int THREAD_COUNT = 4;

    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraSchemaLockTest.class)
            .with(new ThreeNodeCassandraCluster());

    private final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

    @Test
    public void shouldCreateTablesConsistentlyWithMultipleCassandraNodes() throws Exception {
        TableReference table1 = TableReference.createFromFullyQualifiedName("ns.table1");
        CassandraKeyValueServiceConfigManager configManager = CassandraKeyValueServiceConfigManager
                .createSimpleManager(ThreeNodeCassandraCluster.KVS_CONFIG);

        try {
            CyclicBarrier barrier = new CyclicBarrier(THREAD_COUNT);
            for (int i = 0; i < THREAD_COUNT; i++) {
                async(() -> {
                    CassandraKeyValueService keyValueService =
                            CassandraKeyValueService.create(configManager, Optional.empty());
                    barrier.await();
                    keyValueService.createTable(table1, AtlasDbConstants.GENERIC_TABLE_METADATA);
                    return null;
                });
            }
        } finally {
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(4, TimeUnit.MINUTES));
        }

        CassandraKeyValueService kvs =
                CassandraKeyValueService.create(configManager, Optional.empty());
        assertThat(kvs.getAllTableNames(), hasItem(table1));

        assertThat(new File(CONTAINERS.getLogDirectory()),
                containsFiles(everyItem(doesNotContainTheColumnFamilyIdMismatchError())));
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private void async(Callable callable) {
        executorService.submit(callable);
    }

    private static Matcher<File> containsFiles(Matcher<Iterable<File>> fileMatcher) {
        return new FeatureMatcher<File, List<File>>(
                fileMatcher,
                "Directory with files such that",
                "Directory contains") {
            @Override
            protected List<File> featureValueOf(File actual) {
                return ImmutableList.copyOf(actual.listFiles());
            }
        };
    }

    private static Matcher<File> doesNotContainTheColumnFamilyIdMismatchError() {
        return new TypeSafeDiagnosingMatcher<File>() {
            @Override
            protected boolean matchesSafely(File file, Description mismatchDescription) {
                Path path = Paths.get(file.getAbsolutePath());
                try (Stream<String> lines = Files.lines(path, StandardCharsets.ISO_8859_1)) {
                    List<String> badLines = lines.filter(line -> line.contains("Column family ID mismatch"))
                            .collect(Collectors.toList());

                    mismatchDescription
                            .appendText("file called " + file.getAbsolutePath() + " which contains lines")
                            .appendValueList("\n", "\n", "", badLines);

                    return badLines.isEmpty();
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("a file with no column family ID mismatch errors");
            }
        };
    }
}
