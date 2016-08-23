package com.palantir.atlasdb.performance.benchmarks;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.performance.backend.KeyValueServiceConnector;

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
public class KvsGetDynamicBenchmarks {

    private static final String TABLE_NAME_1 = "performance.table2";
    private static final String ROW_COMPONENT = "BIG_ROW_OF_INTS";
    private static final String COLUMN_COMPONENT = "col";
    private static final long DUMMY_TIMESTAMP = 1L;
    private static final long READ_TIMESTAMP = 2L;

    private final int NUM_COLS = 50000;

    private KeyValueServiceConnector connector;
    private KeyValueService kvs;

    private TableReference tableRef1;

    private Map<Cell, Long> allCells2ReadTimestamp;
    private Map<Cell, Long> firstCell2ReadTimestamp;

    @Setup
    public void setup(KeyValueServiceConnector connector) throws UnsupportedEncodingException {
        this.connector = connector;
        kvs = connector.connect();
        tableRef1 = KvsBenchmarks.createTableWithDynamicColumns(kvs, TABLE_NAME_1, ROW_COMPONENT, COLUMN_COMPONENT);
        byte[] rowBytes = ROW_COMPONENT.getBytes("UTF-8");
        Map<Cell,byte[]> values = Maps.newHashMap();
        allCells2ReadTimestamp = Maps.newHashMap();
        firstCell2ReadTimestamp = ImmutableMap.of(Cell.create(rowBytes, ("col_0").getBytes("UTF-8")), READ_TIMESTAMP);
        for (int i = 0; i < NUM_COLS; i++) {
            Cell c = Cell.create(rowBytes, ("col_"+i).getBytes("UTF-8"));
            values.put(c, Ints.toByteArray(i));
            allCells2ReadTimestamp.put(c, READ_TIMESTAMP);
        }
        kvs.put(tableRef1, values, DUMMY_TIMESTAMP);
    }

    @TearDown
    public void cleanup() throws Exception {
        kvs.dropTables(Sets.newHashSet(tableRef1));
        kvs.close();
        connector.close();
    }

    @Benchmark
    public Map<Cell, Value> getAllColumnsExplicitly() {
        Map<Cell, Value> result = kvs.get(tableRef1, allCells2ReadTimestamp);
        KvsBenchmarks.validate(result.size() == NUM_COLS, "Should be %s columns, but were: %s", NUM_COLS, result.size());
        return result;
    }

    @Benchmark
    public Map<Cell, Value> getAllColumnsImplicitly() throws UnsupportedEncodingException {
        Map<Cell, Value> result = kvs.getRows(tableRef1, Collections.singleton(ROW_COMPONENT.getBytes("UTF-8")), ColumnSelection.all(), READ_TIMESTAMP);
        KvsBenchmarks.validate(result.size() == NUM_COLS, "Should be %s columns, but were: %s", NUM_COLS, result.size());
        return result;
    }


    @Benchmark
    public Map<Cell, Value> getFirstColumnExplicitly() {
        Map<Cell, Value> result = kvs.get(tableRef1, firstCell2ReadTimestamp);
        KvsBenchmarks.validate(result.size() == 1, "Should be %s column, but were: %s", 1, result.size());
        int value = Ints.fromByteArray(Iterables.getOnlyElement(result.values()).getContents());
        KvsBenchmarks.validate(value == 0, "Value should be %s but is %s", 0,  value);
        return result;
    }


    @Benchmark
    public Map<Cell, Value> getFirstColumnExplicitlyGetRows() throws UnsupportedEncodingException {
        Map<Cell, Value> result = kvs.getRows(tableRef1, Collections.singleton(ROW_COMPONENT.getBytes("UTF-8")),
                ColumnSelection.create(
                        firstCell2ReadTimestamp.keySet().stream().map(Cell::getColumnName).collect(Collectors.toList())
                ),
                READ_TIMESTAMP);
        KvsBenchmarks.validate(result.size() == 1, "Should be %s column, but were: %s", 1, result.size());
        int value = Ints.fromByteArray(Iterables.getOnlyElement(result.values()).getContents());
        KvsBenchmarks.validate(value == 0, "Value should be %s but is %s", 0,  value);
        return result;
    }

}
