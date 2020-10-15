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
package com.palantir.atlasdb.keyvalue.dbkvs;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.transaction.service.TransactionService;
import java.util.HashMap;
import java.util.Map;

public final class DbKvsTestUtils {
    private DbKvsTestUtils() {
        //utility
    }

    static void setMaxRangeOfTimestampsBatchSize(long value, ConnectionManagerAwareDbKvs kvs) {
        ((DbKvs) kvs.delegate()).setMaxRangeOfTimestampsBatchSize(value);
    }

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
    static void setupTestTable(KeyValueService kvs, TableReference tableName, TransactionService txService) {
        for (int col = 1; col < 10; ++col) {
            Map<Cell, byte[]> toPut = new HashMap<>();
            for (int row = 1; row <= col; ++row) {
                Cell cell = Cell.create(PtBytes.toBytes(row), PtBytes.toBytes(col));
                toPut.put(cell, new byte[] {0});
            }
            for (int ts = 10 * col; ts < 11 * col; ++ts) {
                kvs.put(tableName, toPut, ts);
                if (txService != null) {
                    txService.putUnlessExists(ts, ts);
                }
            }
        }
    }
}
