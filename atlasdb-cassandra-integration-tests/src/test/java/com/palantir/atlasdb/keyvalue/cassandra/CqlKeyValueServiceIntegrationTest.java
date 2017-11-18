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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;

import org.junit.ClassRule;
import org.slf4j.Logger;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueServiceTest;

public class CqlKeyValueServiceIntegrationTest extends AbstractKeyValueServiceTest {
    private static final int FOUR_DAYS_IN_SECONDS = 4 * 24 * 60 * 60;

    private final Logger logger = mock(Logger.class);

//    @ClassRule
//    public static final Containers CONTAINERS = new Containers(CqlKeyValueServiceIntegrationTest.class)
//            .with(new CassandraContainer());

    @Override
    protected KeyValueService getKeyValueService() {
        return createKvs(getConfigWithGcGraceSeconds(FOUR_DAYS_IN_SECONDS), logger);
    }

    @Override
    protected boolean reverseRangesSupported() {
        return false;
    }

    private CqlKeyValueService createKvs(CassandraKeyValueServiceConfig config, Logger testLogger) {
        return CqlKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(config));
    }

    private ImmutableCassandraKeyValueServiceConfig getConfigWithGcGraceSeconds(int gcGraceSeconds) {
        return ImmutableCassandraKeyValueServiceConfig
                .copyOf(CassandraContainer.KVS_CONFIG)
                .withServers(new InetSocketAddress("127.0.0.1", CassandraContainer.CASSANDRA_CQL_PORT))
                .withGcGraceSeconds(gcGraceSeconds);
    }
}
