/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.performance.benchmarks.table;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;

/**
 * State class for creating a single Atlas table with one wide row.
 */
@State(Scope.Benchmark)
public class WideRowTable extends AbstractWideRowsTable {
    public static final int NUM_COLS = 50000;

    public WideRowTable() {
        super(1, NUM_COLS);
    }

    @Override
    public TableReference getTableRef() {
        return Tables.TABLE_REF;
    }

    public Map<Cell, Long> getAllCellsAtMaxTimestamp() {
        return Maps.asMap(getAllCells(), Functions.constant(Long.MAX_VALUE));
    }

    public Map<Cell, Long> getFirstCellAtMaxTimestampAsMap() {
        return ImmutableMap.of(cell(0, 0), Long.MAX_VALUE);
    }

    public Set<Cell> getAllCells() {
        return IntStream.range(0, NUM_COLS).mapToObj(index -> cell(0, index)).collect(Collectors.toSet());
    }

    public Set<Cell> getFirstCellAsSet() {
        return ImmutableSet.of(cell(0, 0));
    }

    public static byte[] getRow() {
        return getRow(0);
    }
}
