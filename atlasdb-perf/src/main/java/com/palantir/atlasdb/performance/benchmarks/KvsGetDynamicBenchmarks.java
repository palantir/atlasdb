package com.palantir.atlasdb.performance.benchmarks;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.performance.backend.KeyValueServiceConnector;

/**
 * Performance benchmarks for KVS put operations.
 *
 * @author mwakerman
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class KvsGetDynamicBenchmarks {

    private static final long VALUE_SEED = 279L;
    private static final String TABLE_NAME_1 = "performance.table1";
    private static final String ROW_COMPONENT = "key";
    private static final String COLUMN_COMPONENT = "col";
    private static final byte [] COLUMN_COMPONENT_IN_BYTES = COLUMN_COMPONENT.getBytes();
    private static final long DUMMY_TIMESTAMP = 1L;
    private static final long READ_TIMESTAMP = 2L;

    private final int NUM_COLS = 100000;

    private KeyValueServiceConnector connector;
    private KeyValueService kvs;
    private Random random = new Random(VALUE_SEED);

    private TableReference tableRef1;

    private List<Cell> allCells;

    @Setup
    public void setup(KeyValueServiceConnector connector) throws UnsupportedEncodingException {
        this.connector = connector;
        kvs = connector.connect();
        tableRef1 = KvsBenchmarks.createTableWithDynamicColumns(kvs, TABLE_NAME_1, ROW_COMPONENT, COLUMN_COMPONENT);
        String rowName = "BIG_ROW_OF_INTS";
        byte[] rowBytes = rowName.getBytes("UTF-8");
        Map<Cell,byte[]> values = Maps.newHashMap();
        allCells = Lists.newArrayList();
        for (int i = 0; i < NUM_COLS; i++) {
            Cell c = Cell.create(rowBytes, ("col_"+i).getBytes("UTF-8"));
            byte[] bytes = Ints.toByteArray(i);
            values.put(c, bytes);
            allCells.add(c);
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
    public void getAllColumns() {
        Map<Cell, Value> result = kvs.get(tableRef1, allCells.stream().collect(Collectors.toMap(c -> c, c -> READ_TIMESTAMP)));
        Validate.isTrue(result.size() == NUM_COLS, String.format("Result.size = %s, NUM_COLS = %s", result.size(), NUM_COLS));
        for (int i = 0; i < NUM_COLS; i++) {
            Cell c = allCells.get(i);
            Value value = result.get(c);
            int resultValue = Ints.fromByteArray(value.getContents());
            Validate.isTrue(resultValue == i, String.format("Result value is %s for iteration %s", resultValue, i));
        }
    }


    @Benchmark
    public void getFirstColumnExplicitly() {
        Map<Cell, Value> result = kvs.get(tableRef1, ImmutableMap.of(allCells.iterator().next(), READ_TIMESTAMP));
        Validate.isTrue(result.size() == 1, "Should be 1 result: " + result.size());
        int resultValue = Ints.fromByteArray(Iterables.getOnlyElement(result.values()).getContents());
        Validate.isTrue(0 == resultValue, "Should be 0: " + resultValue);
    }

}
