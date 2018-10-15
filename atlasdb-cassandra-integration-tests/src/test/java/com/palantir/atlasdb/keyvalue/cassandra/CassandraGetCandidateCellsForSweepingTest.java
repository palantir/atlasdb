/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cassandra.CassandraMutationTimestampProviders;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractGetCandidateCellsForSweepingTest;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;

public class CassandraGetCandidateCellsForSweepingTest extends AbstractGetCandidateCellsForSweepingTest {
    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraKeyValueServiceIntegrationTest.class)
            .with(new CassandraContainer());

    private final MetricsManager metricsManager = MetricsManagers.createForTests();

    @Override
    protected KeyValueService createKeyValueService() {
        return CassandraKeyValueServiceImpl.create(
                metricsManager,
                CassandraContainer.KVS_CONFIG,
                CassandraContainer.LEADER_CONFIG,
                CassandraMutationTimestampProviders.legacyModeForTestsOnly(),
                Mockito.mock(Logger.class));
    }

    @Test
    public void returnCandidateIfPossiblyUncommittedTimestamp() {
        new TestDataBuilder().put(1, 1, 10L).store();
        assertThat(getAllCandidates(conservativeRequest(PtBytes.EMPTY_BYTE_ARRAY, 40L, 1)))
                .containsExactly(ImmutableCandidateCellForSweeping.builder()
                        .cell(cell(1, 1))
                        .sortedTimestamps(ImmutableList.of(10L))
                        .isLatestValueEmpty(false)
                        .build());
    }

    @Test
    public void returnCandidateIfTwoCommittedTimestamps() {
        new TestDataBuilder().put(1, 1, 10L).put(1, 1, 20L).store();
        assertThat(getAllCandidates(conservativeRequest(PtBytes.EMPTY_BYTE_ARRAY, 40L, 1)))
                .containsExactly(ImmutableCandidateCellForSweeping.builder()
                        .cell(cell(1, 1))
                        .sortedTimestamps(ImmutableList.of(10L, 20L))
                        .isLatestValueEmpty(false)
                        .build());
    }

}
