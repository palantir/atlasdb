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
package com.palantir.atlasdb.cleaner;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;

public final class Helper {
    public final KeyValueService keyValueService = new InMemoryKeyValueService(false);
    public final TransactionService transactionService = TransactionServices.createTransactionService(keyValueService);

    public String get(TableReference tableRef, String rowString, String colString, long ts) {
        Cell cell = Cell.create(rowString.getBytes(), colString.getBytes());
        Map<Cell, Value> map = keyValueService.get(tableRef, ImmutableMap.of(cell, ts));
        Value value = map.get(cell);
        if (value == null) {
            return "";
        } else {
            return new String(value.getContents());
        }
    }

    public Set<Long> getAllTimestampsStrings(TableReference tableRef,
                                             String rowString,
                                             String colString,
                                             long ts) {
        return Sets.newHashSet(keyValueService.getAllTimestamps(
                tableRef,
                ImmutableSet.of(Cell.create(rowString.getBytes(), colString.getBytes())), ts).values());
    }

    public void put(TableReference tableRef, String rowString, String colString, String valString, int ts) {
        keyValueService.put(
                tableRef,
                ImmutableMap.of(
                        Cell.create(rowString.getBytes(), colString.getBytes()),
                        valString.getBytes()),
                ts);
    }

    public void close() {
        keyValueService.close();
    }
}
