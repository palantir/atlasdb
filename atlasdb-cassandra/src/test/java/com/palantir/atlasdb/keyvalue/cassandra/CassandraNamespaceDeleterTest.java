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

import static com.palantir.logsafe.testing.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleter;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class CassandraNamespaceDeleterTest {
    private static final String KEYSPACE = "nonreservedkeyspace";
    private static final ImmutableCqlQuery DROP_KEYSPACE_QUERY = CqlQuery.builder()
            .safeQueryFormat(SchemaBuilder.dropKeyspace("\"" + KEYSPACE + "\"")
                    .ifExists()
                    .buildInternal())
            .build();

    @Mock
    private CassandraClient cassandraClient;

    private NamespaceDeleter namespaceDeleter;

    @Before
    public void before() {
        namespaceDeleter = new CassandraNamespaceDeleter(getConfig(), () -> cassandraClient);
    }

    @Test
    public void deleteAllDataFromNamespaceDropsKeyspaceSpecifiedInConfig() throws TException {
        setupCassandraSchemaVersions();

        namespaceDeleter.deleteAllDataFromNamespace();

        verify(cassandraClient).execute_cql3_query(eq(DROP_KEYSPACE_QUERY), any(), any());
    }

    @Test
    public void deleteAllDataFromNamespaceDropsOnlyKeyspaceSpecified() throws TException {
        setupCassandraSchemaVersions();

        namespaceDeleter.deleteAllDataFromNamespace();

        verify(cassandraClient, times(1)).execute_cql3_query(any(), any(), any());
    }

    @Test
    public void deleteAllDataFromNamespaceWaitsForSchemaVersionsBeforeDroppingKeyspace() throws TException {
        when(cassandraClient.describe_schema_versions()).thenReturn(Map.of("hello", List.of(), "world", List.of()));

        assertThatThrownBy(namespaceDeleter::deleteAllDataFromNamespace)
                .hasCauseInstanceOf(IllegalStateException.class)
                .cause()
                .hasMessageContaining("Cassandra cluster cannot come to agreement on schema versions");
        verify(cassandraClient, never()).execute_cql3_query(any(), any(), any());
    }

    @Test
    public void deleteAllDataFromNamespaceWaitsForSchemaVersionsAfterDroppingKeyspace() throws TException {
        when(cassandraClient.describe_schema_versions())
                .thenReturn(Map.of("hello", List.of("world")))
                .thenReturn(Map.of("hello", List.of(), "world", List.of()));

        assertThatThrownBy(namespaceDeleter::deleteAllDataFromNamespace)
                .hasCauseInstanceOf(IllegalStateException.class)
                .cause()
                .hasMessageContaining("Cassandra cluster cannot come to agreement on schema versions");

        verify(cassandraClient).execute_cql3_query(eq(DROP_KEYSPACE_QUERY), any(), any());
    }

    @Test
    public void deleteAllDataFromNamespaceClosesClient() throws TException {
        setupCassandraSchemaVersions();

        namespaceDeleter.deleteAllDataFromNamespace();
        verify(cassandraClient).close();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfNotFoundExceptionThrown() throws TException {
        when(cassandraClient.describe_keyspace(KEYSPACE)).thenThrow(NotFoundException.class);
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsFalseIfKsDefReturned() throws TException {
        when(cassandraClient.describe_keyspace(KEYSPACE)).thenReturn(new KsDef());
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isFalse();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyPropagatesOtherThriftExceptions() throws TException {
        when(cassandraClient.describe_keyspace(KEYSPACE)).thenThrow(TimedOutException.class);
        assertThatThrownBy(namespaceDeleter::isNamespaceDeletedSuccessfully)
                .hasCauseInstanceOf(TimedOutException.class);
    }

    @Test
    public void isNamespaceDeletedSuccessfullyClosesClientWhenNotFound() throws TException {
        when(cassandraClient.describe_keyspace(KEYSPACE)).thenThrow(NotFoundException.class);
        namespaceDeleter.isNamespaceDeletedSuccessfully();
        verify(cassandraClient).close();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyClosesClientWhenKsDefReturned() throws TException {
        when(cassandraClient.describe_keyspace(KEYSPACE)).thenReturn(new KsDef());
        namespaceDeleter.isNamespaceDeletedSuccessfully();
        verify(cassandraClient).close();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyClosesClientOnArbitraryTException() throws TException {
        when(cassandraClient.describe_keyspace(KEYSPACE)).thenThrow(TException.class);
        assertThatThrownBy(namespaceDeleter::isNamespaceDeletedSuccessfully);
        verify(cassandraClient).close();
    }

    @Test
    public void constructorThrowsOnInvalidKeyspaceName() {
        // This test isn't strict on the exception, since we're relying on Namespace validation which uses Validate
        // from commons-lang. This test doesn't necessarily need to be stricter, since rejecting the bad keyspace
        // name is sufficient.

        // Note, this validation probably should happen at the keyspace config level? But it might be too late for
        // that...
        assertThatThrownBy(() -> new CassandraNamespaceDeleter(
                        ImmutableCassandraKeyValueServiceConfig.builder()
                                .from(getConfig())
                                .keyspace("nonreserved; DROP KEYSPACE test;")
                                .build(),
                        () -> cassandraClient))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void setupCassandraSchemaVersions() throws TException {
        when(cassandraClient.describe_schema_versions()).thenReturn(Map.of("hello", List.of("world")));
    }

    private static CassandraKeyValueServiceConfig getConfig() {
        return ImmutableCassandraKeyValueServiceConfig.builder()
                .credentials(ImmutableCassandraCredentialsConfig.builder()
                        .username("test")
                        .password("test-not-real")
                        .build())
                .keyspace(KEYSPACE)
                .schemaMutationTimeoutMillis(10)
                .build();
    }
}
