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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.AsyncKeyValueService;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.async.CassandraAsyncKeyValueServiceFactory;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class CassandraKvsAsyncFallbackMechanismsTests {
    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("ns.pt_kvs_test");
    private static final Cell CELL = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("column"));

    @ClassRule
    public static final CassandraResource CASSANDRA_RESOURCE = new CassandraResource();

    private KeyValueService keyValueService;
    private AsyncKeyValueService asyncKeyValueService;
    private CassandraAsyncKeyValueServiceFactory factory;

    @Before
    public void setUp() {
        asyncKeyValueService = mock(AsyncKeyValueService.class);
        when(asyncKeyValueService.getAsync(any(), any())).thenReturn(Futures.immediateFuture(ImmutableMap.of()));
        factory = mock(CassandraAsyncKeyValueServiceFactory.class);
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
    public void testGetFallBackNotNeeded() {
        when(factory.constructAsyncKeyValueService(any(), any(), eq(AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC)))
                .thenReturn(Optional.of(asyncKeyValueService));

        CassandraKeyValueServiceConfig config = ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CASSANDRA_RESOURCE.getConfig())
                .asyncKeyValueServiceFactory(factory)
                .build();

        keyValueService = CassandraKeyValueServiceImpl.createForTesting(config);
        keyValueService.getAsync(TEST_TABLE, ImmutableMap.of(CELL, 3L));

        verify(asyncKeyValueService, times(1)).getAsync(any(), any());
    }

    @Test
    public void testGetFallingBackToSynchronous() {
        when(factory.constructAsyncKeyValueService(any(), any(), eq(AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC)))
                .thenReturn(Optional.empty());

        CassandraKeyValueServiceConfig config = ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CASSANDRA_RESOURCE.getConfig())
                .asyncKeyValueServiceFactory(factory)
                .build();

        keyValueService = CassandraKeyValueServiceImpl.createForTesting(config);
        keyValueService.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        keyValueService.getAsync(TEST_TABLE, ImmutableMap.of(CELL, 3L));

        verify(asyncKeyValueService, never()).getAsync(any(), any());
    }
}
