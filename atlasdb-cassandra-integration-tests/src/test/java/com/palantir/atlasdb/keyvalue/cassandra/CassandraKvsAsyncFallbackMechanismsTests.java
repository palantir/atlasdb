/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfigTuning;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ReloadingCloseableContainer;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.AsyncKeyValueService;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.async.CassandraAsyncKeyValueServiceFactory;
import com.palantir.atlasdb.keyvalue.cassandra.async.CqlClient;
import com.palantir.atlasdb.keyvalue.cassandra.async.CqlClientImpl;
import com.palantir.atlasdb.keyvalue.cassandra.async.DefaultCassandraAsyncKeyValueServiceFactory;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.ClusterFactory;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.ClusterFactory.CassandraClusterConfig;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CassandraKvsAsyncFallbackMechanismsTests {
    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("ns.pt_kvs_test");
    private static final Cell CELL = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("column"));
    private static final Map<Cell, Long> TIMESTAMP_BY_CELL = ImmutableMap.of(CELL, 3L);

    @ClassRule
    public static final CassandraResource CASSANDRA_RESOURCE = new CassandraResource();

    private static final Refreshable<CassandraKeyValueServiceRuntimeConfig> RUNTIME_CONFIG =
            CASSANDRA_RESOURCE.getRuntimeConfig();

    private KeyValueService keyValueService;

    @Mock
    private AsyncKeyValueService asyncKeyValueService;

    @Mock
    private AsyncKeyValueService throwingAsyncKeyValueService;

    @Mock
    private CassandraAsyncKeyValueServiceFactory factory;

    @After
    public void tearDown() {
        try {
            keyValueService.truncateTables(ImmutableSet.of(TEST_TABLE));
        } catch (Exception e) {
            // this is fine
        }
        CASSANDRA_RESOURCE.registerKvs(keyValueService);
    }

    @Test
    public void testGetAsyncFallBackNotNeeded() {
        when(asyncKeyValueService.getAsync(any(), any())).thenReturn(Futures.immediateFuture(ImmutableMap.of()));
        when(factory.constructAsyncKeyValueService(
                        any(), any(), any(), any(), eq(AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC)))
                .thenReturn(asyncKeyValueService);
        when(asyncKeyValueService.isValid()).thenReturn(true);
        CassandraKeyValueServiceConfig config = ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CASSANDRA_RESOURCE.getConfig())
                .asyncKeyValueServiceFactory(factory)
                .build();

        keyValueService = spy(CassandraKeyValueServiceImpl.createForTesting(config, RUNTIME_CONFIG));
        keyValueService.getAsync(TEST_TABLE, TIMESTAMP_BY_CELL);

        verify(asyncKeyValueService, times(1)).getAsync(any(), any());
        verify(keyValueService, never()).get(any(), any());
    }

    @Test
    public void testGetAsyncFallingBackToSynchronousOnException() {
        when(throwingAsyncKeyValueService.getAsync(any(), any())).thenThrow(new IllegalStateException());
        when(throwingAsyncKeyValueService.isValid()).thenReturn(true);
        when(factory.constructAsyncKeyValueService(
                        any(), any(), any(), any(), eq(AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC)))
                .thenReturn(throwingAsyncKeyValueService);

        CassandraKeyValueServiceConfig config = ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CASSANDRA_RESOURCE.getConfig())
                .asyncKeyValueServiceFactory(factory)
                .build();

        keyValueService = spy(CassandraKeyValueServiceImpl.createForTesting(config, RUNTIME_CONFIG));
        keyValueService.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        keyValueService.getAsync(TEST_TABLE, TIMESTAMP_BY_CELL);

        verify(keyValueService).get(TEST_TABLE, TIMESTAMP_BY_CELL);
    }

    @Test
    public void testGetAsyncFallingBackToSynchronousOnInvalidAsyncKvs() {
        when(factory.constructAsyncKeyValueService(
                        any(), any(), any(), any(), eq(AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC)))
                .thenReturn(asyncKeyValueService);
        when(asyncKeyValueService.isValid()).thenReturn(false);
        CassandraKeyValueServiceConfig config = ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CASSANDRA_RESOURCE.getConfig())
                .asyncKeyValueServiceFactory(factory)
                .build();

        keyValueService = spy(CassandraKeyValueServiceImpl.createForTesting(config, RUNTIME_CONFIG));
        keyValueService.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        keyValueService.getAsync(TEST_TABLE, TIMESTAMP_BY_CELL);

        verify(keyValueService).get(TEST_TABLE, TIMESTAMP_BY_CELL);
    }

    @Test
    public void testGetAsyncFallingBackToSynchronousOnSessionClosedForExecuteAsync()
            throws ExecutionException, InterruptedException {
        CassandraKeyValueServiceConfig config = getConfigWithAsyncFactoryUsingClosedSession(true);

        keyValueService = spy(CassandraKeyValueServiceImpl.createForTesting(config, RUNTIME_CONFIG));
        keyValueService.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        keyValueService.getAsync(TEST_TABLE, TIMESTAMP_BY_CELL).get();

        verify(keyValueService).get(TEST_TABLE, TIMESTAMP_BY_CELL);
    }

    @Test
    public void testGetAsyncFallingBackToSynchronousOnSessionClosedBeforeStatementPreparation() {
        CassandraKeyValueServiceConfig config = getConfigWithAsyncFactoryUsingClosedSession(false);
        keyValueService = spy(CassandraKeyValueServiceImpl.createForTesting(config, RUNTIME_CONFIG));
        keyValueService.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        keyValueService.getAsync(TEST_TABLE, TIMESTAMP_BY_CELL);

        verify(keyValueService).get(TEST_TABLE, TIMESTAMP_BY_CELL);
    }

    private CassandraKeyValueServiceConfig getConfigWithAsyncFactoryUsingClosedSession(
            boolean useSpyPreparedStatement) {
        CassandraKeyValueServiceConfig config = CASSANDRA_RESOURCE.getConfig();
        Cluster cluster = spy(new ClusterFactory(CASSANDRA_RESOURCE.getClusterBuilderWithProxy())
                .constructCluster(CassandraClusterConfig.of(config), config.servers()));
        Session session = spy(cluster.connect());

        doReturn(session).when(cluster).connect();

        if (useSpyPreparedStatement) {
            // Need a real query that uses namespace and table that already exist, irrespective of what the test may
            // create
            PreparedStatement preparedStatement = spy(session.prepare("SELECT COUNT(*) FROM system.schema_columns;"));
            BoundStatement boundStatement = spy(preparedStatement.bind());
            // The prepared statement doesn't use any of the bindings that a normal query would use, so we ignore them.
            doReturn(boundStatement).when(boundStatement).setBytes(any(), any());
            doReturn(boundStatement).when(boundStatement).setLong(anyString(), anyLong());

            doReturn(boundStatement).when(preparedStatement).bind();
            doReturn(preparedStatement).when(session).prepare(anyString());
        }

        session.close();

        CqlClient cqlClient = spy(CqlClientImpl.create(
                new DefaultTaggedMetricRegistry(), cluster, mock(CqlCapableConfigTuning.class), false));

        doReturn(true).when(cqlClient).isValid();
        CassandraAsyncKeyValueServiceFactory cassandraAsyncKeyValueServiceFactory =
                new DefaultCassandraAsyncKeyValueServiceFactory((_ignored1, _ignored2, _ignored3, _ignored4) ->
                        ReloadingCloseableContainer.of(Refreshable.only(0), _ignored -> cqlClient));

        return ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CASSANDRA_RESOURCE.getConfig())
                .asyncKeyValueServiceFactory(cassandraAsyncKeyValueServiceFactory)
                .build();
    }
}
