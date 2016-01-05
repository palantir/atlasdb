/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.transaction.impl;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.Transaction;



public class Transactions {
    private Transactions() { /* */ }

    /**
     * For tables that provide write-write conflict detection, touch is a useful tool.
     * Touch will ensure that the value you read from a table wasn't modified by any concurrent
     * transactions.
     * <p>
     * If a cell is deleted, it also ensures that no concurrent transactions have written a value
     * to that cell.
     * <p>
     * Note: If two concurrent transactions both touch the same cell, only one of them will
     * complete successfully and the other will retry.  This means touch allows a read to have
     * "serializable" semantics, but it is at the cost of a write.  If we find that we rely heavily
     * on touch and it causes a lot of contention, we should consider implementing serializable
     * isolation.
     */
    public static void touchCells(Transaction t, String tableName, Set<Cell> cells) {
        Map<Cell, byte[]> results = Maps.newHashMap(t.get(tableName, cells));
        for (Cell cell : cells) {
            if (!results.containsKey(cell)) {
                results.put(cell, PtBytes.EMPTY_BYTE_ARRAY);
            }
        }
        t.put(tableName, results);
    }

    /**
     * An empty byte array is the same as a deleted value when writing to tables using {@link Transaction}
     */
    public static boolean cellValuesEqual(byte[] v1, byte[] v2) {
        boolean v1Empty = v1 == null || v1.length == 0;
        boolean v2Empty = v2 == null || v2.length == 0;
        if (v1Empty && v2Empty) {
            return true;
        }
        if (v1Empty || v2Empty) {
            return false;
        }
        return Arrays.equals(v1, v2);
    }

}
