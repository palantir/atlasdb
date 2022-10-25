/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.ThriftHostsExtractingVisitor;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientFactory.CassandraClientConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraVerifier.CassandraVerifierConfig;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleter;
import com.palantir.refreshable.Refreshable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.Consumer;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

public final class CassandraNamespaceDeleterIntegrationTest {

    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    private final Refreshable<CassandraKeyValueServiceRuntimeConfig> keyValueServiceRuntimeConfig =
            CASSANDRA.getRuntimeConfig();
    private final CassandraKeyValueServiceConfig keyValueServiceConfigWithNamespaceOne = CASSANDRA.getConfig();
    private final Namespace namespaceOne = Namespace.create(keyValueServiceConfigWithNamespaceOne.getKeyspaceOrThrow());
    private final Namespace namespaceTwo = Namespace.create("test_keyspace");
    private final CassandraKeyValueServiceConfig keyValueServiceConfigForNamespaceTwo =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .from(CASSANDRA.getConfig())
                    .keyspace(namespaceTwo.getName())
                    .build();

    // We must make a fresh KVS and re-initialise to get a new KVS + tables since getDefaultKvs() uses the same KVS for
    // all tests
    // We don't need a kvs for namespaceTwo, because we manually create the keyspace and don't care about the
    // existence of the atlas tables for the new namespace
    private final KeyValueService kvs = CassandraKeyValueServiceImpl.createForTesting(
            keyValueServiceConfigWithNamespaceOne, keyValueServiceRuntimeConfig);
    private final NamespaceDeleter namespaceDeleterForNamespaceOne =
            new CassandraNamespaceDeleter(keyValueServiceConfigWithNamespaceOne, this::createClient);
    private final NamespaceDeleter namespaceDeleterForNamespaceTwo =
            new CassandraNamespaceDeleter(keyValueServiceConfigForNamespaceTwo, this::createClient);

    @After
    public void after() {
        kvs.close();
        namespaceDeleterForNamespaceOne.deleteAllDataFromNamespace();
        namespaceDeleterForNamespaceTwo.deleteAllDataFromNamespace();
    }

    @Test
    public void deleteAllDataFromNamespaceDropsOnlyKeyspaceInConfig() {
        createNamespaceTwo();
        assertNamespaceExists(namespaceOne);
        assertNamespaceExists(namespaceTwo);

        namespaceDeleterForNamespaceOne.deleteAllDataFromNamespace();

        assertNamespaceDoesNotExist(namespaceOne);
        assertNamespaceExists(namespaceTwo);
    }

    @Test
    public void deleteAllDataFromNamespaceDoesNotThrowIfKeyspaceDoesNotExist() {
        assertNamespaceDoesNotExist(namespaceTwo);
        assertThatCode(namespaceDeleterForNamespaceTwo::deleteAllDataFromNamespace)
                .doesNotThrowAnyException();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsFalseIfKeyspaceExists() {
        assertNamespaceExists(namespaceOne);

        assertThat(namespaceDeleterForNamespaceOne.isNamespaceDeletedSuccessfully())
                .isFalse();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfKeyspaceDeleted() {
        assertNamespaceExists(namespaceOne);

        namespaceDeleterForNamespaceOne.deleteAllDataFromNamespace();

        assertNamespaceDoesNotExist(namespaceOne);
        assertThat(namespaceDeleterForNamespaceOne.isNamespaceDeletedSuccessfully())
                .isTrue();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfDifferentKeyspaceExists() {
        namespaceDeleterForNamespaceOne.deleteAllDataFromNamespace();

        createNamespaceTwo();
        assertNamespaceExists(namespaceTwo);

        assertThat(namespaceDeleterForNamespaceOne.isNamespaceDeletedSuccessfully())
                .isTrue();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfKeyspaceNeverCreated() {
        assertNamespaceDoesNotExist(namespaceTwo);

        assertThat(namespaceDeleterForNamespaceTwo.isNamespaceDeletedSuccessfully())
                .isTrue();
    }

    private void assertNamespaceExists(Namespace namespace) {
        runWithClient(client -> Awaitility.await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> assertThatCode(
                        () -> client.describe_keyspace(namespace.getName()))
                .doesNotThrowAnyException()));
    }

    private void assertNamespaceDoesNotExist(Namespace namespace) {
        runWithClient(client -> Awaitility.await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> assertThatThrownBy(
                        () -> client.describe_keyspace(namespace.getName()))
                .isInstanceOf(NotFoundException.class)));
    }

    private void createNamespaceTwo() {
        runWithClient(client -> {
            KsDef ksDef = CassandraVerifier.createKsDefForFresh(
                    client,
                    CassandraVerifierConfig.of(
                            keyValueServiceConfigForNamespaceTwo, keyValueServiceRuntimeConfig.get()));
            try {
                client.system_add_keyspace(ksDef);
            } catch (TException e) {
                throw new RuntimeException("failed to create keyspace", e);
            }
        });
    }

    private void runWithClient(Consumer<CassandraClient> task) {
        try (CassandraClient client = createClient()) {
            task.accept(client);
        }
    }

    private CassandraClient createClient() {
        // The client doesn't depend on the keyspace, which is the fundamental difference between the configs used
        // here and the config for a different keyspace.
        try {
            InetSocketAddress host =
                    keyValueServiceRuntimeConfig.get().servers().accept(ThriftHostsExtractingVisitor.INSTANCE).stream()
                            .findFirst()
                            .orElseThrow();
            return CassandraClientFactory.getClientInternal(
                    CassandraServer.of(host), CassandraClientConfig.of(keyValueServiceConfigWithNamespaceOne));
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
