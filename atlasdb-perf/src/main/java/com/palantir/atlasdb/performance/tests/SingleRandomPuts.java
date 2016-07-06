package com.palantir.atlasdb.performance.tests;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.performance.api.PerformanceTest;
import com.palantir.atlasdb.performance.api.PerformanceTestMetadata;
import com.palantir.atlasdb.performance.generators.RandomByteBufferGenerator;
import com.palantir.atlasdb.table.description.ValueType;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

/**
 * The performance test performs 10,000 single put operations with a randomly generated key and value.
 *
 * @author mwakerman
 */
@PerformanceTestMetadata(name="single-random-puts", version=1)
public class SingleRandomPuts implements PerformanceTest {

    // Constants.
    static private final int NUMBER_OF_PUTS  = 10000;
    static private final int BYTE_ARRAY_SIZE = 100;

    static private final long VALUE_SEED = 279L;

    static private final String TABLE_NAME = "performance.table";
    static private final String ROW_COMPONENT = "key";
    static private final String COLUMN_NAME = "value";
    static private final byte [] COLUMN_NAME_IN_BYTES = COLUMN_NAME.getBytes();

    // Setup.
    private KeyValueService kvs;
    private TableReference tableRef;
    private RandomByteBufferGenerator gen;

    @Override
    public void run() {
        gen.stream().forEach(bytes -> kvs.put(tableRef, createSinglePutValue(bytes), 1));
    }

    @Override
    public void setup(KeyValueService kvs) {
        this.kvs = kvs;
        this.tableRef = TestUtils.createTable(kvs, TABLE_NAME, ROW_COMPONENT, COLUMN_NAME);
        this.gen = RandomByteBufferGenerator.builder()
                    .length(NUMBER_OF_PUTS)
                    .withSeed(VALUE_SEED)
                    .withByteArraySize(BYTE_ARRAY_SIZE)
                    .build();
    }

    @Override
    public void tearDown() {
        kvs.dropTable(tableRef);
    }

    private Map<Cell, byte[]> createSinglePutValue(ByteBuffer bytes) {
        Cell cell = Cell.create(ValueType.STRING.convertFromString(UUID.randomUUID().toString()), COLUMN_NAME_IN_BYTES);
        return ImmutableMap.of(cell, bytes.array());
    }

}
