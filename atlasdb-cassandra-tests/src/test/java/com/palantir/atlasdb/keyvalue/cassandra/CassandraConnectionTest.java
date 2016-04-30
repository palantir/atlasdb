/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.junit.Assert.fail;

import java.net.InetSocketAddress;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.junit.Test;

import com.google.common.base.Optional;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;

public class CassandraConnectionTest {

    private static final CassandraKeyValueServiceConfig NO_CREDS_CKVS_CONFIG = ImmutableCassandraKeyValueServiceConfig
            .builder()
            .addServers(new InetSocketAddress("localhost", 9160))
            .poolSize(20)
            .keyspace("atlasdb")
            .credentials(Optional.absent())
            .ssl(false)
            .replicationFactor(1)
            .mutationBatchCount(10000)
            .mutationBatchSizeBytes(10000000)
            .fetchBatchCount(1000)
            .safetyDisabled(false)
            .autoRefreshNodes(false)
            .build();
            
    
    @Test
    public void testAuthProvided() {
        CassandraKeyValueService kv = CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraTestSuite.CKVS_CONFIG));
        kv.teardown();
        assert true; // getting here implies authentication succeeded
    }

    @Test
    public void testAuthMissing() {
        try {
            CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(NO_CREDS_CKVS_CONFIG));
            fail();
        } catch (RuntimeException e) {
            boolean threwIRE = false;
            Throwable t = e.getCause();
            while (!threwIRE && t != null) {
                threwIRE |= t instanceof InvalidRequestException;
                t = t.getCause();
            }
            assert threwIRE;
            return;
        }
        fail();
    }
}
