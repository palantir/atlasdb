/**
 * Copyright 2015 Palantir Technologies
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

import org.junit.Ignore;

import com.google.common.net.HostAndPort;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.AbstractSerializableTransactionTest;

public class CQLKeyValueServiceSerializableTransactionTest extends
        AbstractSerializableTransactionTest {

    @Override
    protected KeyValueService getKeyValueService() {
        return CQLKeyValueService.create(ImmutableCassandraKeyValueServiceConfig.builder()
                .addServers(IpAndPort.from(HostAndPort.fromHost("localhost").withDefaultPort(9160)))
                .port(9160)
                .poolSize(20)
                .keyspace("atlasdb")
                .ssl(false)
                .replicationFactor(1)
                .mutationBatchCount(10000)
                .mutationBatchSizeBytes(10000000)
                .fetchBatchCount(1000)
                .safetyDisabled(false)
                .autoRefreshNodes(false)
                .build());
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }

    @Override
    @Ignore
    public void testRangePaging() {
    }

}
