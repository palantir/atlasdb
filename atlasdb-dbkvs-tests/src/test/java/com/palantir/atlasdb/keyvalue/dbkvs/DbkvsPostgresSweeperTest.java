/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.AbstractSweeperTest;

public class DbkvsPostgresSweeperTest extends AbstractSweeperTest {
    @Override
    protected KeyValueService getKeyValueService() {
        return ConnectionManagerAwareDbKvs.create(DbkvsPostgresTestSuite.getKvsConfig());
    }

    @Test
    public void wholeTableInOneBatch() {
        setupTestTable();
        SweepResults results = sweep(TABLE_NAME, 100, 10, 1000);
        Assert.assertFalse(results.getNextStartRow().isPresent());
        Assert.assertEquals(9, results.getCellsExamined());
        Assert.assertEquals(240, results.getCellsDeleted());
    }

    @Test
    public void smallCellBatches() {
        setupTestTable();
        SweepResults results = sweep(TABLE_NAME, 100, 10, 7);
        Assert.assertFalse(results.getNextStartRow().isPresent());
        Assert.assertEquals(9, results.getCellsExamined());
        Assert.assertEquals(240, results.getCellsDeleted());
    }

    @Test
    public void smallRangeOfTsMaxBatch() {
        setupTestTable();
        setGetRangeOfTsMaxBatch(5);
        SweepResults results = sweep(TABLE_NAME, 100, 10, 1000);
        Assert.assertFalse(results.getNextStartRow().isPresent());
        Assert.assertEquals(9, results.getCellsExamined());
        Assert.assertEquals(240, results.getCellsDeleted());
        resetGetRangeOfTsMaxBatch();
    }

    @Test
    public void bothRangesSmallAndSmallerTimestamp() {
        setupTestTable();
        setGetRangeOfTsMaxBatch(7);
        SweepResults results = sweep(TABLE_NAME, 96, 10, 3);
        Assert.assertFalse(results.getNextStartRow().isPresent());
        Assert.assertEquals(9, results.getCellsExamined());
        Assert.assertEquals(213, results.getCellsDeleted());
        resetGetRangeOfTsMaxBatch();
    }

    @Test
    public void smallRowBatch() {
        setupTestTable();
        setGetRangeOfTsMaxBatch(5);
        SweepResults results = sweep(TABLE_NAME, 100, 5, 1000);
        Assert.assertTrue(results.getNextStartRow().isPresent());
        Assert.assertEquals(5, results.getCellsExamined());
        Assert.assertEquals(170, results.getCellsDeleted());
        resetGetRangeOfTsMaxBatch();
    }


    private void setGetRangeOfTsMaxBatch(long value) {
        ((DbKvs) ((ConnectionManagerAwareDbKvs) getKeyValueService()).delegate()).setGetRangeOfTsMaxBatch(value);
    }

    private void resetGetRangeOfTsMaxBatch() {
        setGetRangeOfTsMaxBatch(DbKvs.INITIAL_GET_RANGE_OF_TS_BATCH);
    }

    /*
    Table for testing different batching strategies. Has 9 rows cells to be examined and 240 entries to be deleted
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
    private void setupTestTable() {
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
