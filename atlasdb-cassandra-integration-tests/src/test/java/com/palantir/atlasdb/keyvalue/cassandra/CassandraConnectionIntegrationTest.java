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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Optional;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;

public class CassandraConnectionIntegrationTest {
    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraConnectionIntegrationTest.class)
            .with(new CassandraContainer());

    private static final CassandraKeyValueServiceConfig NO_CREDS_CKVS_CONFIG = ImmutableCassandraKeyValueServiceConfig
            .copyOf(CassandraContainer.THRIFT_CONFIG)
            .withCredentials(Optional.absent());

    @Test
    public void testAuthProvided() {
        CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.THRIFT_CONFIG),
                CassandraContainer.LEADER_CONFIG).close();
    }

    // Don't worry about failing this test if you're running against a local Cassandra that isn't setup with auth magic
    @Test
    public void testAuthMissing() {
        try {
            CassandraKeyValueService.create(
                    CassandraKeyValueServiceConfigManager.createSimpleManager(NO_CREDS_CKVS_CONFIG),
                    CassandraContainer.LEADER_CONFIG);
            fail();
        } catch (RuntimeException e) {
            boolean threwInvalidRequestException = false;
            Throwable cause = e.getCause();
            while (!threwInvalidRequestException && cause != null) {
                threwInvalidRequestException = cause instanceof InvalidRequestException;
                cause = cause.getCause();
            }
            assertTrue(threwInvalidRequestException);
            return;
        }
        fail();
    }
}
