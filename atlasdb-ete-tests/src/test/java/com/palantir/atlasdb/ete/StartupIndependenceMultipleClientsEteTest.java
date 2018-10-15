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
package com.palantir.atlasdb.ete;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.containers.CassandraEnvironment;

public class StartupIndependenceMultipleClientsEteTest {
    private static final List<String> ALL_CASSANDRA_NODES = ImmutableList.of("cassandra");
    private static final List<String> CLIENTS = ImmutableList.of("ete1", "ete2", "ete3");

    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupWithoutWaiting(
            StartupIndependenceMultipleClientsEteTest.class,
            "docker-compose.startup-multiple-clients-cassandra.yml",
            CLIENTS,
            CassandraEnvironment.get());

    @Before
    public void setUp() throws IOException, InterruptedException {
        StartupIndependenceUtils.randomizeNamespace();
        StartupIndependenceUtils.killCassandraNodes(ALL_CASSANDRA_NODES);
    }

    @Test
    public void atlasStartsWithCassandraDownAndInitializesAsynchronously()
            throws IOException, InterruptedException {
        StartupIndependenceUtils.restartAtlasWithChecks();
        StartupIndependenceUtils.assertNotInitializedExceptionIsThrownAndMappedCorrectly();
        StartupIndependenceUtils.startCassandraNodes(ALL_CASSANDRA_NODES);
        StartupIndependenceUtils.assertSatisfiedWithin(240, StartupIndependenceUtils::canPerformTransaction);
    }
}
