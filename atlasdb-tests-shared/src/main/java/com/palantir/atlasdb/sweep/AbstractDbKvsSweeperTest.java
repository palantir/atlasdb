/**
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
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;

public abstract class AbstractDbKvsSweeperTest extends AbstractSweeperTest {
    @Test
    public void wholeTableInOneBatch() {
        setupTestTable();
        SweepResults results = sweep(TABLE_NAME, 100, 10, 1000);
        assertThat(results.getNextStartRow().isPresent()).isFalse();
        assertThat(results.getCellsExamined()).isEqualTo(9);
        assertThat(results.getCellsDeleted()).isEqualTo(240);
    }

    @Test
    public void smallCellBatches() {
        setupTestTable();
        SweepResults results = sweep(TABLE_NAME, 100, 10, 7);
        assertThat(results.getNextStartRow().isPresent()).isFalse();
        assertThat(results.getCellsExamined()).isEqualTo(9);
        assertThat(results.getCellsDeleted()).isEqualTo(240);
    }

    @Test
    public void smallRangeOfTsMaxBatch() {
        setupTestTable();
        setGetRangeOfTsMaxBatch(5);
        SweepResults results = sweep(TABLE_NAME, 100, 10, 1000);
        assertThat(results.getNextStartRow().isPresent()).isFalse();
        assertThat(results.getCellsExamined()).isEqualTo(9);
        assertThat(results.getCellsDeleted()).isEqualTo(240);
        resetGetRangeOfTsMaxBatch();
    }

    @Test
    public void bothRangesSmallAndSmallerTimestamp() {
        setupTestTable();
        setGetRangeOfTsMaxBatch(7);
        SweepResults results = sweep(TABLE_NAME, 96, 10, 3);
        assertThat(results.getNextStartRow().isPresent()).isFalse();
        assertThat(results.getCellsExamined()).isEqualTo(9);
        assertThat(results.getCellsDeleted()).isEqualTo(213);
        resetGetRangeOfTsMaxBatch();
    }

    @Test
    public void smallRowBatch() {
        setupTestTable();
        setGetRangeOfTsMaxBatch(5);
        SweepResults results = sweep(TABLE_NAME, 100, 5, 1000);
        assertThat(results.getNextStartRow().isPresent()).isTrue();
        assertThat(results.getCellsExamined()).isEqualTo(5);
        assertThat(results.getCellsDeleted()).isEqualTo(170);
        resetGetRangeOfTsMaxBatch();
    }

    protected abstract void setGetRangeOfTsMaxBatch(long value);

    protected abstract void resetGetRangeOfTsMaxBatch();

    /*
    Table for testing different batching strategies. Has 9 rows to be examined and 240 entries to be deleted
        ------------------------------------------------------
       | (10) | (20, 21) | (30, 31, 32) | ... | (90, ... , 98)|
       |------------------------------------------------------|
       |  --  | (20, 21) | (30, 31, 32) | ... | (90, ... , 98)|
       |------------------------------------------------------|
       |                        ...                           |
       |------------------------------------------------------|
       |  --  |    --    |      --      | ... | (90, ... , 98)|
        ------------------------------------------------------
     */
    public void setupTestTable() {
        createTable(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
        for (int col = 1; col < 10; ++col) {
            Map<Cell, byte[]> toPut = new HashMap<>();
            for (int row = 1; row <= col; ++row) {
                Cell cell = Cell.create(PtBytes.toBytes(row), PtBytes.toBytes(col));
                toPut.put(cell, new byte[]{0});
            }
            for (int ts = 10 * col; ts < 11 * col; ++ts) {
                kvs.put(TABLE_NAME, toPut, ts);
                txService.putUnlessExists(ts, ts);
            }
        }
    }
}
