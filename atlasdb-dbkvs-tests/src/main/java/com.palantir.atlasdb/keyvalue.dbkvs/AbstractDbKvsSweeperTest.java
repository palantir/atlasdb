/*
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.dbkvs;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.AbstractSweeperTest;

public abstract class AbstractDbKvsSweeperTest extends AbstractSweeperTest {
    @After
    public void resetMaxBatch() {
        setMaxRangeOfTimestampsBatchSize(DbKvs.DEFAULT_GET_RANGE_OF_TS_BATCH);
    }

    @Test
    public void wholeTableInOneBatch() {
        setupTestTable();
        checkResults(sweep(TABLE_NAME, 100L, 10, 100), false, 9, 240L);
    }

    @Test
    public void smallCellBatches() {
        setupTestTable();
        checkResults(sweep(TABLE_NAME, 100L, 10, 7), false, 9, 240L);
    }

    @Test
    public void smallRangeOfTsMaxBatch() {
        setupTestTable();
        setMaxRangeOfTimestampsBatchSize(5L);
        checkResults(sweep(TABLE_NAME, 100L, 10, 1000), false, 9, 240L);
    }

    @Test
    public void bothRangesSmallAndSmallerTimestamp() {
        setupTestTable();
        setMaxRangeOfTimestampsBatchSize(7L);
        checkResults(sweep(TABLE_NAME, 96L, 10, 3), false, 9, 213L);
    }

    @Test
    public void smallRowBatch() {
        setupTestTable();
        setMaxRangeOfTimestampsBatchSize(5L);
        checkResults(sweep(TABLE_NAME, 100L, 5, 1000), true, 5, 170L);
    }

    private void checkResults(SweepResults results, boolean moreRows, int rowsExamined, long cellsDeleted) {
        assertThat(results.getNextStartRow().isPresent()).isEqualTo(moreRows);
        assertThat(results.getCellsExamined()).isEqualTo(rowsExamined);
        assertThat(results.getCellsDeleted()).isEqualTo(cellsDeleted);
    }

    private void setMaxRangeOfTimestampsBatchSize(long value) {
        DbKvsTestUtils.setMaxRangeOfTimestampsBatchSize(value,
                (ConnectionManagerAwareDbKvs) ((SweepStatsKeyValueService) kvs).delegate());
    }

    private void setupTestTable() {
        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
        DbKvsTestUtils.setupTestTable(kvs, TABLE_NAME, txService);
    }
}
