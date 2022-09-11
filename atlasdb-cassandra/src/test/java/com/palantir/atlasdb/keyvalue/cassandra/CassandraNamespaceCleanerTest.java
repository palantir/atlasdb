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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.palantir.atlasdb.NamespaceCleaner;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class CassandraNamespaceCleanerTest {
    private static final String KEYSPACE = "nonreservedkeyspace";

    @Mock
    private CassandraClient cassandraClient;

    @Captor
    ArgumentCaptor<CqlQuery> cqlQueryArgumentCaptor;

    private NamespaceCleaner namespaceCleaner;

    @Before
    public void before() {
        namespaceCleaner = new CassandraNamespaceCleaner(getConfig(), () -> cassandraClient);
    }

    @Test
    public void dropAllTablesDropsKeyspaceSpecifiedInConfig() throws TException {
        prepareClientMock();
        namespaceCleaner.dropAllTables();
        verify(cassandraClient)
                .execute_cql3_query(
                        eq(CqlQuery.builder()
                                .safeQueryFormat(SchemaBuilder.dropKeyspace(KEYSPACE)
                                        .ifExists()
                                        .buildInternal())
                                .build()),
                        any(),
                        any());
    }

    @Test
    public void dropAllTablesDropsOnlyKeyspaceSpecified() throws TException {
        prepareClientMock();
        namespaceCleaner.dropAllTables();
        verify(cassandraClient, times(1)).execute_cql3_query(any(), any(), any());
    }

    @Test
    public void dropAllTablesWaitsForSchemaVersionsBeforeDroppingKeyspace() throws TException {
        when(cassandraClient.describe_schema_versions()).thenReturn(Map.of("hello", List.of(), "world", List.of()));
        assertThatThrownBy(namespaceCleaner::dropAllTables)
                .hasCauseInstanceOf(IllegalStateException.class)
                .cause()
                .hasMessageContaining("Cassandra cluster cannot come to agreement on schema versions");
        verify(cassandraClient, never()).execute_cql3_query(any(), any(), any());
    }

    @Test
    public void dropAllTablesWaitsForSchemaVersionsAfterDroppingKeyspace() throws TException {
        when(cassandraClient.describe_schema_versions())
                .thenReturn(Map.of("hello", List.of("world")))
                .thenReturn(Map.of("hello", List.of(), "world", List.of()));
        assertThatThrownBy(namespaceCleaner::dropAllTables)
                .hasCauseInstanceOf(IllegalStateException.class)
                .cause()
                .hasMessageContaining("Cassandra cluster cannot come to agreement on schema versions");
        verify(cassandraClient)
                .execute_cql3_query(
                        eq(CqlQuery.builder()
                                .safeQueryFormat(SchemaBuilder.dropKeyspace(KEYSPACE)
                                        .ifExists()
                                        .buildInternal())
                                .build()),
                        any(),
                        any());
    }

    @Test
    public void dropAllTablesClosesClient() throws TException {
        prepareClientMock();
        namespaceCleaner.dropAllTables();
        verify(cassandraClient).close();
    }

    @Test
    public void areAllTablesSuccessfullyDroppedReturnsTrueIfNotFoundExceptionThrown() throws TException {
        when(cassandraClient.describe_keyspace(KEYSPACE)).thenThrow(NotFoundException.class);
        assertThat(namespaceCleaner.areAllTablesSuccessfullyDropped()).isTrue();
    }

    @Test
    public void areAllTablesSuccessfullyDroppedReturnsFalseIfKsDefReturned() throws TException {
        when(cassandraClient.describe_keyspace(KEYSPACE)).thenReturn(new KsDef());
        assertThat(namespaceCleaner.areAllTablesSuccessfullyDropped()).isFalse();
    }

    @Test
    public void areAllTablesSuccessfullyDroppedPropagatesOtherThriftExceptions() throws TException {
        when(cassandraClient.describe_keyspace(KEYSPACE)).thenThrow(TimedOutException.class);
        assertThatThrownBy(namespaceCleaner::areAllTablesSuccessfullyDropped)
                .hasCauseInstanceOf(TimedOutException.class);
    }

    @Test
    public void areAllTablesSuccessfullyDroppedClosesClientWhenNotFound() throws TException {
        when(cassandraClient.describe_keyspace(KEYSPACE)).thenThrow(NotFoundException.class);
        namespaceCleaner.areAllTablesSuccessfullyDropped();
        verify(cassandraClient).close();
    }

    @Test
    public void areAllTablesSuccessfullyDroppedClosesClientWhenKsDefReturned() throws TException {
        when(cassandraClient.describe_keyspace(KEYSPACE)).thenReturn(new KsDef());
        namespaceCleaner.areAllTablesSuccessfullyDropped();
        verify(cassandraClient).close();
    }

    @Test
    public void areAllTablesSuccessfullyDroppedClosesClientOnArbitraryTException() throws TException {
        when(cassandraClient.describe_keyspace(KEYSPACE)).thenThrow(TException.class);
        assertThatThrownBy(namespaceCleaner::areAllTablesSuccessfullyDropped);
        verify(cassandraClient).close();
    }

    // @Test
    // public void dropAllTablesCannotHaveArbitraryCqlInjected() throws TException {
    //     namespaceCleaner = new CassandraNamespaceCleaner(
    //             ImmutableCassandraKeyValueServiceConfig.builder()
    //                     .from(getConfig())
    //                     .keyspace("nonreserved; DROP KEYSPACE test;")
    //                     .build(),
    //             () -> cassandraClient);
    //     prepareClientMock();
    //     namespaceCleaner.dropAllTables();
    //     verify(cassandraClient).execute_cql3_query(cqlQueryArgumentCaptor.capture(), any(), any());
    //     assertThat(cqlQueryArgumentCaptor.getValue().safeQueryFormat()).isEqualTo("test");
    // }

    // Test that we wait for schema versions, that we're not at risk of cql injection, that we close the client

    private void prepareClientMock() throws TException {
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
