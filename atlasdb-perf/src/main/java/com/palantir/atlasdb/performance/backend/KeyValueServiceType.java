/**
 * Copyright 2016 Palantir Technologies
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
 *
 */

package com.palantir.atlasdb.performance.backend;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutablePostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.ImmutablePostgresConnectionConfig;

public enum KeyValueServiceType implements KeyValueServiceTypeInterface{
    POSTGRES(5432, "postgres-docker-compose.yml"),
    CASSANDRA(9160, "cassandra-docker-compose.yml");

    private final int kvsPort;
    private final String dockerComposeFileName;

    KeyValueServiceType(int kvsPort, String dockerComposeFileName) {
        this.kvsPort = kvsPort;
        this.dockerComposeFileName = dockerComposeFileName;
    }
    @Override
    public String getDockerComposeResourceFileName() {
        return dockerComposeFileName;
    }

    @Override
    public int getKeyValueServicePort() {
        return kvsPort;
    }

    @Override
    public KeyValueServiceConfig getKeyValueServiceConfig(InetSocketAddress addr) {
        switch (this) {
            case POSTGRES:
                return ImmutableDbKeyValueServiceConfig.builder()
                        .ddl(ImmutablePostgresDdlConfig.builder().build())
                        .connection(
                                ImmutablePostgresConnectionConfig.builder()
                                        .host(addr.getHostString())
                                        .port(5432)
                                        .dbName("atlas")
                                        .dbLogin("palantir")
                                        .dbPassword(ImmutableMaskedValue.of("palantir"))
                                        .build()
                        ).build();
            case CASSANDRA:
                return ImmutableCassandraKeyValueServiceConfig.builder()
                        .addServers(addr)
                        .poolSize(20)
                        .keyspace("atlasdb")
                        .credentials(ImmutableCassandraCredentialsConfig.builder()
                                .username("cassandra")
                                .password("cassandra")
                                .build())
                        .ssl(false)
                        .replicationFactor(1)
                        .mutationBatchCount(10000)
                        .mutationBatchSizeBytes(10000000)
                        .fetchBatchCount(1000)
                        .safetyDisabled(false)
                        .autoRefreshNodes(false)
                        .build();
            default:
                throw new UnsupportedOperationException("Unable to get the KVS config for ");
        }
    }

    @Override
    public boolean canConnect(InetSocketAddress addr) {
        switch (this) {
            case POSTGRES:
                return true;
            case CASSANDRA:
                try {
                    CassandraKeyValueService.create(
                            CassandraKeyValueServiceConfigManager.createSimpleManager(
                                    (CassandraKeyValueServiceConfig) getKeyValueServiceConfig(addr)),
                            Optional.of(ImmutableLeaderConfig
                                    .builder()
                                    .quorumSize(1)
                                    .localServer(addr.getHostString())
                                    .leaders(ImmutableSet.of(addr.getHostString()))
                                    .build()));
                    return true;
                } catch (Exception e) {
                    return false;
                }
            default:
                throw new UnsupportedOperationException("Trying to check connection for unknown KVS ");
        }
    }


    private static Map< String, KeyValueServiceTypeInterface > map =
            new TreeMap< String, KeyValueServiceTypeInterface >();

    static {
        for (KeyValueServiceType backend : values()) {
            map.put(backend.toString(), backend);
        }
    }

    public static KeyValueServiceTypeInterface KeyValueServiceTypeFor(String backend) {
        return map.get(backend);
    }

    public static void addNewBackendType(KeyValueServiceTypeInterface backend) {
        if (!map.containsKey(backend.toString())) {
            map.put(backend.toString(), backend);
        }
    }


}
