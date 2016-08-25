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

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;
import com.palantir.atlasdb.services.AtlasDbServices;

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

    private static final String TABLE_NAME = "performance.table";
    private static final String ROW_COMPONENT = "BIG_ROW_OF_INTS";
    private static final String COLUMN_COMPONENT = "col";
    private static final byte[] ROW_BYTES = ROW_COMPONENT.getBytes(StandardCharsets.UTF_8);

    private static final int NUM_COLS = 50000;

    private AtlasDbServicesConnector connector;
    private AtlasDbServices services;

    private TableReference tableRef;

    private Set<Cell> allCells;
    private Set<Cell> firstCell;

    @Setup
    public void setup(AtlasDbServicesConnector conn) throws UnsupportedEncodingException {
        this.connector = conn;
        services = conn.connect();
        tableRef = Benchmarks.createTableWithDynamicColumns(
                services.getKeyValueService(), TABLE_NAME, ROW_COMPONENT, COLUMN_COMPONENT);
        storeData();
    }

    private void storeData() {
        services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> values = Maps.newHashMap();
            allCells = Sets.newHashSet();
            firstCell = Sets.newHashSet(cell(0));
            for (int i = 0; i < NUM_COLS; i++) {
                values.put(cell(i), Ints.toByteArray(i));
            }
            allCells = values.keySet();
            txn.put(this.tableRef, values);
            return null;
        });
    }

    private Cell cell(int cellNum) {
        return Cell.create(ROW_BYTES, ("col_" + cellNum).getBytes(StandardCharsets.UTF_8));
    }

    @TearDown
    public void cleanup() throws Exception {
        services.getKeyValueService().dropTables(Sets.newHashSet(tableRef));
        connector.close();
    }

    @Benchmark
    public Map<Cell, byte[]> getAllColumnsExplicitly() {
        return services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> result = txn.get(this.tableRef, allCells);
            Benchmarks.validate(result.values().size() == NUM_COLS,
                    "Should be %s columns, but were: %s", NUM_COLS, result.values().size());
            return result;
        });
    }

    @Benchmark
    public SortedMap<byte[], RowResult<byte[]>> getAllColumnsImplicitly() throws UnsupportedEncodingException {
        return services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            SortedMap<byte[], RowResult<byte[]>> result = txn.getRows(this.tableRef,
                    Collections.singleton(ROW_BYTES),
                    ColumnSelection.all());
            int count = Iterables.getOnlyElement(result.values()).getColumns().size();
            Benchmarks.validate(count == NUM_COLS, "Should be %s columns, but were: %s", NUM_COLS, count);
            return result;
        });
    }

    @Benchmark
    public Map<Cell, byte[]> getFirstColumnExplicitly() {
        return services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> result = txn.get(this.tableRef, firstCell);
            Benchmarks.validate(result.values().size() == 1,
                    "Should be %s column, but were: %s", 1, result.values().size());
            int value = Ints.fromByteArray(Iterables.getOnlyElement(result.values()));
            Benchmarks.validate(value == 0, "Value should be %s but is %s", 0,  value);
            return result;
        });
    }

    @Benchmark
    public SortedMap<byte[], RowResult<byte[]>> getFirstColumnExplicitlyGetRows() throws UnsupportedEncodingException {
        return services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            SortedMap<byte[], RowResult<byte[]>> result = txn.getRows(this.tableRef,
                    Collections.singleton(ROW_BYTES),
                    ColumnSelection.create(
                            firstCell.stream().map(Cell::getColumnName).collect(Collectors.toList())
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
