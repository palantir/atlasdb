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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
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
import org.junit.After;
import org.junit.Before;
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

    private KeyValueService keyValueService;

    @Mock
    private AsyncKeyValueService asyncKeyValueService;

    @Mock
    private AsyncKeyValueService throwingAsyncKeyValueService;

    @Mock
    private CassandraAsyncKeyValueServiceFactory factory;

    @Before
    public void setUp() {
        lenient()
                .when(asyncKeyValueService.getAsync(any(), any()))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of()));
        lenient().when(throwingAsyncKeyValueService.getAsync(any(), any())).thenThrow(new IllegalStateException());
    }

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
        when(factory.constructAsyncKeyValueService(
                        any(), any(), any(), any(), eq(AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC)))
                .thenReturn(asyncKeyValueService);
        when(asyncKeyValueService.isValid()).thenReturn(true);
        CassandraKeyValueServiceConfig config = ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CASSANDRA_RESOURCE.getConfig())
                .asyncKeyValueServiceFactory(factory)
                .build();

        keyValueService = CassandraKeyValueServiceImpl.createForTesting(config);
        keyValueService.getAsync(TEST_TABLE, TIMESTAMP_BY_CELL);

        verify(asyncKeyValueService, times(1)).getAsync(any(), any());
    }

    @Test
    public void testGetAsyncFallingBackToSynchronousOnException() {
        when(factory.constructAsyncKeyValueService(
                        any(), any(), any(), any(), eq(AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC)))
                .thenReturn(throwingAsyncKeyValueService);

        CassandraKeyValueServiceConfig config = ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CASSANDRA_RESOURCE.getConfig())
                .asyncKeyValueServiceFactory(factory)
                .build();

        keyValueService = spy(CassandraKeyValueServiceImpl.createForTesting(config));
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

        keyValueService = spy(CassandraKeyValueServiceImpl.createForTesting(config));
        keyValueService.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        keyValueService.getAsync(TEST_TABLE, TIMESTAMP_BY_CELL);

        verify(keyValueService).get(TEST_TABLE, TIMESTAMP_BY_CELL);
    }

    //TODO: Cleanup!! DO NOT MERGE
    @Test
    public void testGetAsyncFallingBackToSynchronousOnSessionClosed() {
        when(asyncKeyValueService.isValid()).thenReturn(false);
        CassandraKeyValueServiceConfig config = CASSANDRA_RESOURCE.getConfig();
        Cluster cluster = spy(new ClusterFactory(Cluster::builder)
                .constructCluster(CassandraClusterConfig.of(config), config.servers()));
        Session session = spy(cluster.connect());
        when(session.isClosed()).thenReturn(true);
        when(cluster.connect()).thenReturn(session);
        session.close();
        CqlClient cqlClient = CqlClientImpl.create(
                new DefaultTaggedMetricRegistry(), cluster, mock(CqlCapableConfigTuning.class), false);
        CassandraAsyncKeyValueServiceFactory cassandraAsyncKeyValueServiceFactory =
                new DefaultCassandraAsyncKeyValueServiceFactory((_ignored1, _ignored2, _ignored3, _ignored4) ->
                        ReloadingCloseableContainer.of(Refreshable.only(0), _ignored -> cqlClient));

        CassandraKeyValueServiceConfig configWithNewFactory = ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CASSANDRA_RESOURCE.getConfig())
                .asyncKeyValueServiceFactory(cassandraAsyncKeyValueServiceFactory)
                .build();

        keyValueService = spy(CassandraKeyValueServiceImpl.createForTesting(configWithNewFactory));
        keyValueService.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        keyValueService.getAsync(TEST_TABLE, TIMESTAMP_BY_CELL);

        verify(keyValueService).get(TEST_TABLE, TIMESTAMP_BY_CELL);
    }
}
