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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfigTuning;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CqlClientImplTest {
    private static final boolean INITIALIZE_ASYNC = false;
    private static final TaggedMetricRegistry REGISTRY = new DefaultTaggedMetricRegistry();

    @Mock
    private Cluster cluster;

    @Mock
    private Session session;

    @Mock
    private CqlCapableConfigTuning cqlCapableConfigTuning;

    @Before
    public void setUp() {
        when(cluster.connect()).thenReturn(session);
    }

    @Test
    public void cqlClientInvalidWhenSessionIsClosed() {
        CqlClient cqlClient = CqlClientImpl.create(REGISTRY, cluster, cqlCapableConfigTuning, INITIALIZE_ASYNC);
        when(session.isClosed()).thenReturn(true);
        assertThat(cqlClient.isValid()).isFalse();
    }

    @Test
    public void cqlClientValidWhenSessionOpen() {
        CqlClient cqlClient = CqlClientImpl.create(REGISTRY, cluster, cqlCapableConfigTuning, INITIALIZE_ASYNC);
        when(session.isClosed()).thenReturn(false);
        assertThat(cqlClient.isValid()).isTrue();
    }
}
