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
package com.palantir.atlasdb.transaction.impl;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

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
    public static void touchCells(Transaction t, TableReference tableRef, Set<Cell> cells) {
        Map<Cell, byte[]> results = Maps.newHashMap(t.get(tableRef, cells));
        for (Cell cell : cells) {
            if (!results.containsKey(cell)) {
                results.put(cell, PtBytes.EMPTY_BYTE_ARRAY);
            }
        }
        t.put(tableRef, results);
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
