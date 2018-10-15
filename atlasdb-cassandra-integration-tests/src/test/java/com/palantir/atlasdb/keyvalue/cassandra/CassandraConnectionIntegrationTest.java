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

import java.util.Optional;

import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;

public class CassandraConnectionIntegrationTest {
    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraConnectionIntegrationTest.class)
            .with(new CassandraContainer());

    private static final CassandraKeyValueServiceConfig NO_CREDS_CKVS_CONFIG = ImmutableCassandraKeyValueServiceConfig
            .copyOf(CassandraContainer.KVS_CONFIG)
            .withCredentials(Optional.empty());

    @Test
    public void testAuthProvided() {
        CassandraKeyValueServiceImpl.createForTesting(
                CassandraContainer.KVS_CONFIG,
                CassandraContainer.LEADER_CONFIG).close();
    }

    @Test
    public void testAuthMissing() {
        CassandraKeyValueServiceImpl.createForTesting(
                NO_CREDS_CKVS_CONFIG,
                CassandraContainer.LEADER_CONFIG).close();
    }
}
