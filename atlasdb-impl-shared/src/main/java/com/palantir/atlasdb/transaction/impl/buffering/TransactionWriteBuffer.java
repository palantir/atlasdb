/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.buffering;

import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.BiConsumer;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public interface TransactionWriteBuffer {
    long byteCount();
    void putWrites(TableReference tableRef, Map<Cell, byte[]> values);
    void applyPostCondition(BiConsumer<TableReference, Map<Cell, byte[]>> postCondition);
    Collection<Cell> writtenCells(TableReference tableRef);
    Iterable<TableReference> tablesWrittenTo();
    Map<TableReference, ? extends Map<Cell, byte[]>> all();
    SortedMap<Cell, byte[]> writesByTable(TableReference tableRef);
    boolean hasWrites();
    Multimap<Cell, TableReference> cellsToScrubByCell();
    Multimap<TableReference, Cell> cellsToScrubByTable();
}
