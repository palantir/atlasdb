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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OraclePrefixedTableNames;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.DbKvsGetCandidateCellsForSweeping;
import com.palantir.atlasdb.keyvalue.impl.AbstractGetCandidateCellsForSweepingTest;
import com.palantir.common.base.ClosableIterator;

public class DbKvsOracleGetCandidateCellsForSweepingTest extends AbstractGetCandidateCellsForSweepingTest {

    private static DbKeyValueServiceConfig config;
    private static SqlConnectionSupplier connectionSupplier;

    @Override
    protected KeyValueService createKeyValueService() {
        config = DbkvsOracleTestSuite.getKvsConfig();
        ConnectionManagerAwareDbKvs kvs = ConnectionManagerAwareDbKvs.create(config);
        connectionSupplier = kvs.getSqlConnectionSupplier();
        return kvs;
    }

    @Test
    public void singleCellSpanningSeveralPages() {
        new TestDataBuilder()
                .put(10, 1, 1000)
                .put(10, 1, 1001)
                .put(10, 1, 1002)
                .put(10, 1, 1003)
                .put(10, 1, 1004)
                .store();
        List<CandidateCellForSweeping> cells = getWithOverriddenLimit(
                conservativeRequest(PtBytes.EMPTY_BYTE_ARRAY, 2000L, Long.MIN_VALUE), 2);
        assertEquals(ImmutableList.of(ImmutableCandidateCellForSweeping.builder()
                .cell(cell(10, 1))
                .isLatestValueEmpty(false)
                .numCellsTsPairsExamined(5)
                .sortedTimestamps(1000L, 1001L, 1002L, 1003L, 1004L)
                .build()), cells);
    }

    @Test
    public void returnFirstAndLastCellOfThePage() {
        new TestDataBuilder()
                .put(10, 1, 1000)
                .put(10, 2, 400)
                // The cell (20, 1) is not a candidate because the minimumUncommittedTimestamp is 750, which is greater
                // than 500. However, we still need to return this cell since it's at the page boundary.
                .put(20, 1, 500)
                // <---- page boundary here ---->
                // Again, this cell is not a candidate, but we need to return it
                // since it's the first SQL row in the page.
                .put(30, 1, 600)
                .store();
        List<CandidateCellForSweeping> cells = getWithOverriddenLimit(
                conservativeRequest(PtBytes.EMPTY_BYTE_ARRAY, 2000L, 750L), 3);
        assertEquals(
                ImmutableList.of(
                    ImmutableCandidateCellForSweeping.builder()
                        .cell(cell(10, 1))
                        .isLatestValueEmpty(false)
                        .numCellsTsPairsExamined(1)
                        .sortedTimestamps(1000L)
                        .build(),
                    ImmutableCandidateCellForSweeping.builder()
                        .cell(cell(20, 1))
                        .isLatestValueEmpty(false)
                        .numCellsTsPairsExamined(3)
                        // No timestamps because the cell is not a real candidate
                        .sortedTimestamps()
                        .build(),
                    ImmutableCandidateCellForSweeping.builder()
                        .cell(cell(30, 1))
                        .isLatestValueEmpty(false)
                        .numCellsTsPairsExamined(4)
                        // No timestamps because the cell is not a real candidate
                        .sortedTimestamps()
                        .build()),
                cells);
    }

    private List<CandidateCellForSweeping> getWithOverriddenLimit(
            CandidateCellForSweepingRequest request,
            int sqlRowLimitOverride) {
        try (ClosableIterator<List<CandidateCellForSweeping>> iter = createImpl(sqlRowLimitOverride)
                    .getCandidateCellsForSweeping(TEST_TABLE, request, null)) {
            return ImmutableList.copyOf(Iterators.concat(Iterators.transform(iter, List::iterator)));
        }
    }

    private DbKvsGetCandidateCellsForSweeping createImpl(int sqlRowLimitOverride) {
        return new OracleGetCandidateCellsForSweeping(
                new OraclePrefixedTableNames(new OracleTableNameGetter((OracleDdlConfig) config.ddl())),
                connectionSupplier,
                x -> sqlRowLimitOverride);
    }

}
