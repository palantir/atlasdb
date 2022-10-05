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

import com.palantir.atlasdb.NamespaceCleaner;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.ThriftHostsExtractingVisitor;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientFactory.CassandraClientConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraVerifier.CassandraVerifierConfig;
import com.palantir.refreshable.Refreshable;
import java.net.InetSocketAddress;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class CassandraNamespaceCleanerIntegrationTest {

    @ClassRule
    private static final CassandraResource CASSANDRA = new CassandraResource();

    private final Refreshable<CassandraKeyValueServiceRuntimeConfig> keyValueServiceRuntimeConfig =
            CASSANDRA.getRuntimeConfig();
    private final CassandraKeyValueServiceConfig keyValueServiceConfig = CASSANDRA.getConfig();

    private final Namespace namespace = Namespace.create(keyValueServiceConfig.getKeyspaceOrThrow());
    private final Namespace differentNamespace = Namespace.create(keyValueServiceConfig.getKeyspaceOrThrow());
    private final CassandraKeyValueServiceConfig keyValueServiceConfigForDifferentKeyspace =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .from(CASSANDRA.getConfig())
                    .keyspace(differentNamespace.getName())
                    .build();

    private final KeyValueService kvs = CASSANDRA.getDefaultKvs();
    private NamespaceCleaner namespaceCleaner;
    private NamespaceCleaner namespaceCleanerForAnotherKeyspace;
    private CassandraClient client;

    @Before
    public void before() throws TException {
        InetSocketAddress host =
                keyValueServiceRuntimeConfig.get().servers().accept(ThriftHostsExtractingVisitor.INSTANCE).stream()
                        .findFirst()
                        .orElseThrow();
        client = CassandraClientFactory.getClientInternal(host, CassandraClientConfig.of(keyValueServiceConfig));
        namespaceCleaner = new CassandraNamespaceCleaner(keyValueServiceConfig, () -> client);
        namespaceCleanerForAnotherKeyspace =
                new CassandraNamespaceCleaner(keyValueServiceConfigForDifferentKeyspace, () -> client);
    }

    @After
    public void after() {
        namespaceCleaner.deleteAllDataFromNamespace();
        namespaceCleanerForAnotherKeyspace.deleteAllDataFromNamespace();
    }

    @Test
    public void dropNamespaceDropsOnlyKeyspaceInConfig() {
        createDifferentKeyspace();
        assertNamespaceExists(namespace);
        assertNamespaceExists(differentNamespace);
        namespaceCleaner.deleteAllDataFromNamespace();
        assertNamespaceDoesNotExist(namespace);
        assertNamespaceExists(differentNamespace);
    }

    @Test
    public void dropNamespaceDoesNotThrowIfKeyspaceDoesNotExist() {
        assertNamespaceDoesNotExist(differentNamespace);
        assertThatCode(namespaceCleanerForAnotherKeyspace::deleteAllDataFromNamespace)
                .doesNotThrowAnyException();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsFalseIfKeyspaceRemain() {
        assertNamespaceExists(namespace);
        assertThat(namespaceCleaner.isNamespaceDeletedSuccessfully()).isFalse();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfKeyspaceDeleted() {
        createDifferentKeyspace();
        assertNamespaceExists(differentNamespace);
        assertNamespaceExists(namespace);
        namespaceCleaner.deleteAllDataFromNamespace();

        assertNamespaceDoesNotExist(namespace);
        assertThat(namespaceCleaner.isNamespaceDeletedSuccessfully()).isFalse();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfKeyspaceNeverCreated() {
        assertNamespaceDoesNotExist(differentNamespace);
        assertThat(namespaceCleanerForAnotherKeyspace.isNamespaceDeletedSuccessfully())
                .isFalse();
    }

    // TODO (awaitility)

    private void assertNamespaceExists(Namespace namespace) {
        assertThatCode(() -> client.describe_keyspace(namespace.getName())).doesNotThrowAnyException();
    }

    private void assertNamespaceDoesNotExist(Namespace namespace) {
        assertThatThrownBy(() -> client.describe_keyspace(namespace.getName())).isInstanceOf(NotFoundException.class);
    }

    private void createDifferentKeyspace() {
        CassandraVerifier.createKsDefForFresh(
                client,
                CassandraVerifierConfig.of(
                        keyValueServiceConfigForDifferentKeyspace, keyValueServiceRuntimeConfig.get()));
    }
}
