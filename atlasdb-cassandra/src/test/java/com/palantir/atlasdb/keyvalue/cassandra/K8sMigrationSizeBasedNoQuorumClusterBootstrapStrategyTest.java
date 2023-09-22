/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTopologyValidator.ClusterTopologyResult;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class K8sMigrationSizeBasedNoQuorumClusterBootstrapStrategyTest {
    private final AtomicReference<CassandraServersConfig> config = new AtomicReference<>();
    private final NoQuorumClusterBootstrapStrategy strategy =
            new K8sMigrationSizeBasedNoQuorumClusterBootstrapStrategy(config::get);

    @Test
    public void returnsNoQuorumIfNoResultsProvided() {
        assertThat(strategy.accept(ImmutableMap.of())).isEqualTo(ClusterTopologyResult.noQuorum());
    }

    @Test
    public void returnsNoQuorumIfOnlyHardFailuresProvided() {
        // TODO
    }

    @Test
    public void returnsNoQuorumIfOnlySoftFailuresProvided() {
        // TODO
    }

    @Test
    public void returnsDissentIfTwoTopologiesAvailable() {
        // TODO
    }

    @Test
    public void returnsDissentIfThreeTopologiesAvailable() {
        // TODO
    }

    @Test
    public void returnsNoQuorumIfHostIdsSizeDoesNotMatchConfig() {
        // TODO
    }

    @Test
    public void returnsNoQuorumIfNoPossibleQuorumInOriginalCloud() {
        // TODO
    }

    @Test
    public void returnsConsensusIfAllConditionsSatisfied() {
        // TODO
    }
}
