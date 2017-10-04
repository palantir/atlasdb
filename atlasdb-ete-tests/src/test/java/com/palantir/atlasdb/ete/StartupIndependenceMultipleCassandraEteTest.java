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

package com.palantir.atlasdb.ete;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.containers.CassandraEnvironment;

public class StartupIndependenceMultipleCassandraEteTest {
    private static final List<String> ALL_CASSANDRA_NODES = ImmutableList.of("cassandra1", "cassandra2", "cassandra3");
    private static final List<String> QUORUM_OF_CASSANDRA_NODES = ImmutableList.of("cassandra1", "cassandra2");
    private static final List<String> ONE_CASSANDRA_NODE = ImmutableList.of("cassandra1");
    private static final List<String> CLIENTS = ImmutableList.of("ete1");

    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupWithoutWaiting(
            StartupIndependenceMultipleCassandraEteTest.class,
            "docker-compose.startup-independence.cassandra.yml",
            CLIENTS,
            CassandraEnvironment.get());

    @Before
    public void setUp() throws IOException, InterruptedException {
        StartupIndependenceUtils.randomizeNamespace();
        StartupIndependenceUtils.killCassandraNodes(ALL_CASSANDRA_NODES);
    }

    @Test
    public void atlasStartsWithCassandraDownAndInitializesWithAllNodes()
            throws IOException, InterruptedException {
        StartupIndependenceUtils.restartAtlasWithChecks();
        StartupIndependenceUtils.assertNotInitializedExceptionIsThrownAndMappedCorrectly();
        StartupIndependenceUtils.startCassandraNodes(ALL_CASSANDRA_NODES);
        StartupIndependenceUtils.assertSatisfiedWithin(240, StartupIndependenceUtils::canPerformTransaction);
    }

    @Test
    public void atlasStartsWithCassandraDownAndInitializesWithQuorum()
            throws IOException, InterruptedException {
        StartupIndependenceUtils.restartAtlasWithChecks();
        StartupIndependenceUtils.assertNotInitializedExceptionIsThrownAndMappedCorrectly();
        StartupIndependenceUtils.startCassandraNodes(QUORUM_OF_CASSANDRA_NODES);
        StartupIndependenceUtils.assertSatisfiedWithin(240, StartupIndependenceUtils::canPerformTransaction);
    }

    @Test
    public void atlasInitializesSynchronouslyIfCassandraIsInGoodState() throws InterruptedException, IOException {
        StartupIndependenceUtils.startCassandraNodes(ALL_CASSANDRA_NODES);
        StartupIndependenceUtils.verifyCassandraIsSettled();
        StartupIndependenceUtils.restartAtlasWithChecks();
        assertTrue(StartupIndependenceUtils.canPerformTransaction());

        StartupIndependenceUtils.killCassandraNodes(ONE_CASSANDRA_NODE);
        StartupIndependenceUtils.restartAtlasWithChecks();
        assertTrue(StartupIndependenceUtils.canPerformTransaction());
    }
}
