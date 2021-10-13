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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.AbstractBackgroundSweeperIntegrationTest;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.timelock.paxos.InMemoryTimelockServices;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class CassandraBackgroundSweeperIntegrationTest extends AbstractBackgroundSweeperIntegrationTest {
    private CassandraResource cassandra;
    private InMemoryTimelockServices services;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        services = InMemoryTimelockServices.create(tempFolder);
        cassandra = new CassandraResource(() -> createKeyValueService(services));
    }

    @After
    public void after() {
        services.close();
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return cassandra.getDefaultKvs();
    }

    private KeyValueService createKeyValueService(InMemoryTimelockServices services) {
        return CassandraKeyValueServiceImpl.create(
                MetricsManagers.createForTests(),
                cassandra.getConfig(),
                CassandraTestTools.getMutationProviderWithStartingTimestamp(1_000_000, services));
    }
}
