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

package com.palantir.atlasdb.performance.benchmarks;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.performance.benchmarks.table.WideRowTable;

/**
 * Performance benchmarks for KVS get with dynamic columns.
 *
 * @author coda
 *
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 30, timeUnit = TimeUnit.SECONDS)
public class TransactionGetDynamicBenchmarks {

    @Benchmark
    public Map<Cell, byte[]> getAllColumnsExplicitly(WideRowTable table) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> result = txn.get(WideRowTable.TABLE_REF, table.getAllCells());
            Benchmarks.validate(result.values().size() == WideRowTable.NUM_COLS,
                    "Should be %s columns, but were: %s", WideRowTable.NUM_COLS, result.values().size());
            return result;
        });
    }

    @Benchmark
    public SortedMap<byte[], RowResult<byte[]>> getAllColumnsImplicitly(WideRowTable table) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            SortedMap<byte[], RowResult<byte[]>> result = txn.getRows(WideRowTable.TABLE_REF,
                    Collections.singleton(WideRowTable.ROW_BYTES.array()),
                    ColumnSelection.all());
            int count = Iterables.getOnlyElement(result.values()).getColumns().size();
            Benchmarks.validate(count == WideRowTable.NUM_COLS,
                    "Should be %s columns, but were: %s", WideRowTable.NUM_COLS, count);
            return result;
        });
    }

    @Benchmark
    public Map<Cell, byte[]> getFirstColumnExplicitly(WideRowTable table) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> result = txn.get(WideRowTable.TABLE_REF, table.getFirstCellAsSet());
            Benchmarks.validate(result.values().size() == 1,
                    "Should be %s column, but were: %s", 1, result.values().size());
            int value = Ints.fromByteArray(Iterables.getOnlyElement(result.values()));
            Benchmarks.validate(value == 0, "Value should be %s but is %s", 0,  value);
            return result;
        });
    }

    @Benchmark
    public SortedMap<byte[], RowResult<byte[]>> getFirstColumnExplicitlyGetRows(WideRowTable table) {
        return table.getTransactionManager().runTaskThrowOnConflict(txn -> {
            SortedMap<byte[], RowResult<byte[]>> result = txn.getRows(WideRowTable.TABLE_REF,
                    Collections.singleton(WideRowTable.ROW_BYTES.array()),
                    ColumnSelection.create(
                            table.getFirstCellAsSet().stream().map(Cell::getColumnName).collect(Collectors.toList())
                    ));
            int count = Iterables.getOnlyElement(result.values()).getColumns().size();
            Benchmarks.validate(count == 1, "Should be %s column, but were: %s", 1, count);
            int value = Ints.fromByteArray(
                    Iterables.getOnlyElement(
                            Iterables.getOnlyElement(result.values()).getColumns().entrySet()
                    ).getValue());
            Benchmarks.validate(value == 0, "Value should be %s but is %s", 0,  value);
            return result;
        });
    }

}
