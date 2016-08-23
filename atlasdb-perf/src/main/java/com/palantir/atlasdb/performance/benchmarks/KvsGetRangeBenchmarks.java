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
 *
 */

package com.palantir.atlasdb.performance.benchmarks;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 30, timeUnit = TimeUnit.SECONDS)
public class KvsGetRangeBenchmarks {

    private static final String TABLE_NAME_1 = "performance.table1";
    private static final String ROW_COMPONENT = "key";
    private static final String COLUMN_NAME = "value";
    private static final byte [] COLUMN_NAME_IN_BYTES = COLUMN_NAME.getBytes(StandardCharsets.UTF_8);
    private static final long DUMMY_TIMESTAMP = 1L;
    private static final long QUERY_TIMESTAMP = 2L;

    private static final int VALUE_BYTE_ARRAY_SIZE = 100;
    private static final long VALUE_SEED = 279L;

    private AtlasDbServicesConnector connector;
    private KeyValueService kvs;
    private Random random = new Random(VALUE_SEED);

    private TableReference tableRef1;

    private static final int NUM_ROWS = 10000;
    private static final int PUT_BATCH_SIZE = 1000;
    private static final int NUM_REQUESTS = 1000;

    @Setup(Level.Trial)
    public void setup(AtlasDbServicesConnector conn) {
        this.connector = conn;
        this.kvs = conn.connect().getKeyValueService();
        this.tableRef1 = Benchmarks.createTable(kvs, TABLE_NAME_1, ROW_COMPONENT, COLUMN_NAME);
        storeData();
    }


    private void storeData() {
        Validate.isTrue(NUM_ROWS % PUT_BATCH_SIZE  == 0);
        for (int i = 0; i < NUM_ROWS; i += PUT_BATCH_SIZE) {
            Map<TableReference, Map<Cell, byte[]>> multiPutMap = Maps.newHashMap();
            multiPutMap.put(tableRef1, generateBatch(i, PUT_BATCH_SIZE));
            kvs.multiPut(multiPutMap, DUMMY_TIMESTAMP);
        }
    }

    private byte[] generateValue() {
        byte[] value = new byte[VALUE_BYTE_ARRAY_SIZE];
        random.nextBytes(value);
        return value;
    }

    private Map<Cell, byte[]> generateBatch(int startKey, int size) {
        Map<Cell, byte[]> map = Maps.newHashMapWithExpectedSize(size);
        for (int j = 0; j < size; j++) {
            byte[] key = Ints.toByteArray(startKey + j);
            byte[] value = generateValue();
            map.put(Cell.create(key, COLUMN_NAME_IN_BYTES), value);
        }
        return map;
    }

    @TearDown(Level.Trial)
    public void cleanup() throws Exception {
        this.kvs.dropTables(Sets.newHashSet(tableRef1));
        this.kvs.close();
        this.connector.close();
        this.tableRef1 = null;
    }


    @Benchmark
    public void getSingleRange() {
        int startRow = random.nextInt(NUM_ROWS);
        int endRow = startRow + 1;
        RangeRequest request = RangeRequest.builder()
                .batchHint(1)
                .startRowInclusive(Ints.toByteArray(startRow))
                .endRowExclusive(Ints.toByteArray(endRow))
                .build();
        ClosableIterator<RowResult<Value>> result = kvs.getRange(this.tableRef1, request, QUERY_TIMESTAMP);
        ArrayList<RowResult<Value>> list = Lists.newArrayList(result);
        byte[] rowName = Iterables.getOnlyElement(list).getRowName();
        int rowNumber = Ints.fromByteArray(rowName);
        Benchmarks.validate(rowNumber == startRow, "Start Row %s, row number %s", startRow, rowNumber);
    }

    private Iterable<RangeRequest> getRangeRequests(int numRequests) {
        List<RangeRequest> requests = Lists.newArrayList();
        Set<Integer> used = Sets.newHashSet();
        for (int i = 0; i < numRequests; i++) {
            int startRow;
            do {
                startRow = random.nextInt(NUM_ROWS);
            } while (used.contains(startRow));
            int endRow = startRow + 1;
            RangeRequest request = RangeRequest.builder()
                    .batchHint(2)
                    .startRowInclusive(Ints.toByteArray(startRow))
                    .endRowExclusive(Ints.toByteArray(endRow))
                    .build();
            requests.add(request);
            used.add(startRow);
        }
        return requests;
    }

    @Benchmark
    public void getMultiRange() {
        Iterable<RangeRequest> requests = getRangeRequests(NUM_REQUESTS);
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> results =
                kvs.getFirstBatchForRanges(tableRef1, requests, QUERY_TIMESTAMP);

        int numRequests = Iterables.size(requests);

        Benchmarks.validate(numRequests == results.size(),
                "Got %s requests and %s results, requests %s, results %s",
                numRequests, results.size(), requests, results);

        results.forEach((request, result) -> {
            Benchmarks.validate(1 == result.getResults().size(), "Key %s, List size is %s",
                    Ints.fromByteArray(request.getStartInclusive()), result.getResults().size());
            Benchmarks.validate(!result.moreResultsAvailable(), "Key %s, result.moreResultsAvailable() %s",
                    Ints.fromByteArray(request.getStartInclusive()), result.moreResultsAvailable());
            RowResult<Value> row = Iterables.getOnlyElement(result.getResults());
            Benchmarks.validate(Arrays.equals(request.getStartInclusive(), row.getRowName()),
                    "Request row is %s, result is %s",
                    Ints.fromByteArray(request.getStartInclusive()),
                    Ints.fromByteArray(row.getRowName()));
        });
    }

}
