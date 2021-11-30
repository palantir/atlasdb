/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.backup.CassandraRepairHelper;
import com.palantir.atlasdb.cassandra.backup.LightweightOppTokenRange;
import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class CassandraRepairEteTest {
    private CassandraRepairHelper cassandraRepairHelper;

    @Before
    public void setUp() {
        MetricsManager metricsManager =
                new MetricsManager(new MetricRegistry(), new DefaultTaggedMetricRegistry(), _unused -> true);

        // TODO(gs): RF2
        CassandraKeyValueServiceConfig config = ThreeNodeCassandraCluster.KVS_CONFIG;
        CassandraKeyValueService kvs = CassandraKeyValueServiceImpl.createForTesting(config);
        cassandraRepairHelper = new CassandraRepairHelper(metricsManager, _unused -> config, _unused -> kvs);
    }

    @Test
    public void shouldGetATokenRange() {
        Map<InetSocketAddress, Set<LightweightOppTokenRange>> ranges =
                cassandraRepairHelper.getRangesToRepair(Namespace.of("namespace"), "doesNotMatter");
        assertThat(ranges).isNotEmpty();
    }
}
