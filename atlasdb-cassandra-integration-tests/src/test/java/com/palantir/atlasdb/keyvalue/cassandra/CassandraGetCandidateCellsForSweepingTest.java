/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
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
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractGetCandidateCellsForSweepingTest;

public class CassandraGetCandidateCellsForSweepingTest extends AbstractGetCandidateCellsForSweepingTest {
    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraKeyValueServiceIntegrationTest.class)
            .with(new CassandraContainer());

    @Override
    protected KeyValueService createKeyValueService() {
        return CassandraKeyValueServiceImpl.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.KVS_CONFIG),
                CassandraContainer.LEADER_CONFIG,
                Mockito.mock(Logger.class));
    }

    @Test
    public void returnCandidateIfPossiblyUncommittedTimestamp() {
        new TestDataBuilder().put(1, 1, 10L).store();
        assertThat(getAllCandidates(conservativeRequest(PtBytes.EMPTY_BYTE_ARRAY, 40L, 5)))
                .containsExactly(ImmutableCandidateCellForSweeping.builder()
                        .cell(cell(1, 1))
                        .sortedTimestamps(new long[] { 10L })
                        .isLatestValueEmpty(false)
                        .numCellsTsPairsExamined(1)
                        .build());
    }

    @Test
    public void returnCandidateIfTwoCommittedTimestamps() {
        new TestDataBuilder().put(1, 1, 10L).put(1, 1, 20L).store();
        assertThat(getAllCandidates(conservativeRequest(PtBytes.EMPTY_BYTE_ARRAY, 40L, 30)))
                .containsExactly(ImmutableCandidateCellForSweeping.builder()
                        .cell(cell(1, 1))
                        .sortedTimestamps(new long[] { 10L, 20L })
                        .isLatestValueEmpty(false)
                        .numCellsTsPairsExamined(2)
                        .build());
    }

    // TODO(nziebart): check that this is ok to fail, and delete
    @Ignore
    @Test
    public void doNotReturnCandidateWithCommitedEmptyValueIfConservative() {
        new TestDataBuilder().putEmpty(1, 1, 10L).store();
        assertThat(getAllCandidates(conservativeRequest(PtBytes.EMPTY_BYTE_ARRAY, 40L, 30))).isEmpty();
    }

    @Test
    public void returnCandidateWithCommitedEmptyValueIfThorough() {
        new TestDataBuilder().putEmpty(1, 1, 10L).store();
        assertThat(getAllCandidates(thoroughRequest(PtBytes.EMPTY_BYTE_ARRAY, 40L, 30)))
                .containsExactly(ImmutableCandidateCellForSweeping.builder()
                        .cell(cell(1, 1))
                        .sortedTimestamps(new long[] { 10L })
                        .isLatestValueEmpty(true)
                        .numCellsTsPairsExamined(1)
                        .build());
    }
}
