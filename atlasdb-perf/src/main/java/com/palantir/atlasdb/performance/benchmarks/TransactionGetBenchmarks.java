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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.Validate;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
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
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitables;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 30, timeUnit = TimeUnit.SECONDS)
public class TransactionGetBenchmarks {

    private static final String TABLE_NAME = "performance.table";
    private static final String ROW_COMPONENT = "key";
    private static final String COLUMN_NAME = "value";
    private static final byte [] COLUMN_NAME_IN_BYTES = COLUMN_NAME.getBytes(StandardCharsets.UTF_8);

    private static final int VALUE_BYTE_ARRAY_SIZE = 100;
    private static final long VALUE_SEED = 279L;


    private static final int NUM_ROWS = 10000;
    private static final int PUT_BATCH_SIZE = 1000;

    private static final int GET_CELLS_SIZE = 500;
    private static final int RANGE_REQUEST_SIZE = 500;

    private static final int NUM_RANGE_REQUESTS = 50;
    private static final int RANGES_SINGLE_REQUEST_SIZE = 10;

    private Random random = new Random(VALUE_SEED);

    private AtlasDbServicesConnector connector;
    private AtlasDbServices services;
    private TableReference tableRef;

    @Setup(Level.Trial)
    public void setup(AtlasDbServicesConnector conn) {
        this.connector = conn;
        this.services = conn.connect();
        this.tableRef = Benchmarks.createTable(services.getKeyValueService(), TABLE_NAME, ROW_COMPONENT, COLUMN_NAME);
        storeData();
    }

    private void storeData() {
        Validate.isTrue(NUM_ROWS % PUT_BATCH_SIZE  == 0);
        services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            for (int i = 0; i < NUM_ROWS; i += PUT_BATCH_SIZE) {
                Map<Cell, byte[]> putMap = generateBatch(i, PUT_BATCH_SIZE);
                txn.put(tableRef, putMap);
            }
            return null;
        });
    }

    private byte[] generateValue() {
        byte[] value = new byte[VALUE_BYTE_ARRAY_SIZE];
        random.nextBytes(value);
        return value;
    }

    private Map<Cell, byte[]> generateBatch(int startKey, int size) {
        Map<Cell, byte[]> map = Maps.newHashMapWithExpectedSize(size);
        for (int j = 0; j < size; j++) {
            byte[] value = generateValue();
            map.put(cell(startKey + j), value);
        }
        return map;
    }

    private Cell cell(int i) {
        byte[] key = Ints.toByteArray(i);
        return Cell.create(key, COLUMN_NAME_IN_BYTES);
    }

    private int rowNumber(byte[] row) {
        return Ints.fromByteArray(row);
    }

    @TearDown(Level.Trial)
    public void cleanup() throws Exception {
        this.services.getKeyValueService().dropTables(Sets.newHashSet(tableRef));
        this.connector.close();
        this.tableRef = null;
    }


    private Set<Cell> getCellsRequest(int numberOfCellsToRequest) {
        Set<Cell> ret = Sets.newHashSet();
        while (ret.size() < numberOfCellsToRequest) {
            ret.add(cell(random.nextInt(NUM_ROWS)));
        }
        return ret;
    }

    @Benchmark
    public void getSingleCell() {
        services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Set<Cell> request = getCellsRequest(1);
            Map<Cell, byte[]> result = txn.get(this.tableRef, request);
            byte[] rowName = Iterables.getOnlyElement(result.entrySet()).getKey().getRowName();
            int rowNumber = Ints.fromByteArray(rowName);
            int expectRowNumber = rowNumber(Iterables.getOnlyElement(request).getRowName());
            Benchmarks.validate(rowNumber == expectRowNumber,
                    "Start Row %s, row number %s", expectRowNumber, rowNumber);
            return null;
        });
    }

    @Benchmark
    public void getCells() {
        services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            Set<Cell> request = getCellsRequest(GET_CELLS_SIZE);
            Map<Cell, byte[]> result = txn.get(this.tableRef, request);
            Benchmarks.validate(result.size() == GET_CELLS_SIZE,
                    "expected %s cells, found %s cells", GET_CELLS_SIZE, result.size());
            return null;
        });
    }

    private RangeRequest getRangeRequest(int numberOfRowsToRequest) {
        int startRow = random.nextInt(NUM_ROWS - numberOfRowsToRequest + 1);
        int endRow = startRow + numberOfRowsToRequest;
        return RangeRequest.builder()
                .batchHint(1)
                .startRowInclusive(Ints.toByteArray(startRow))
                .endRowExclusive(Ints.toByteArray(endRow))
                .build();
    }

    @Benchmark
    public void getSingleCellWithRangeQuery() {
        services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            RangeRequest request = getRangeRequest(1);
            List<RowResult<byte[]>> result = BatchingVisitables.copyToList(txn.getRange(this.tableRef, request));
            byte[] rowName = Iterables.getOnlyElement(result).getRowName();
            int rowNumber = rowNumber(rowName);
            int expectedRowNumber = rowNumber(request.getStartInclusive());
            Benchmarks.validate(rowNumber == expectedRowNumber,
                    "Start Row %s, row number %s", expectedRowNumber, rowNumber);
            return null;
        });
    }

    @Benchmark
    public void getRange() {
        services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            RangeRequest request = getRangeRequest(RANGE_REQUEST_SIZE);
            List<RowResult<byte[]>> results = BatchingVisitables.copyToList(txn.getRange(this.tableRef, request));
            Benchmarks.validate(results.size() == RANGE_REQUEST_SIZE,
                    "Expected %s rows, found %s rows", RANGE_REQUEST_SIZE, results.size());
            return null;
        });
    }

    @Benchmark
    public void getRanges() {
        services.getTransactionManager().runTaskThrowOnConflict(txn -> {
            List<RangeRequest> requests = Stream
                    .generate(() -> getRangeRequest(RANGES_SINGLE_REQUEST_SIZE))
                    .limit(NUM_RANGE_REQUESTS)
                    .collect(Collectors.toList());
            Iterable<BatchingVisitable<RowResult<byte[]>>> results = txn.getRanges(this.tableRef, requests);
            results.forEach(bvs -> {
                List<RowResult<byte[]>> result = BatchingVisitables.copyToList(bvs);
                Benchmarks.validate(result.size() == RANGES_SINGLE_REQUEST_SIZE,
                        "Expected %s rows, found %s rows", RANGES_SINGLE_REQUEST_SIZE, result.size());
            });
            return null;
        });
    }

}
