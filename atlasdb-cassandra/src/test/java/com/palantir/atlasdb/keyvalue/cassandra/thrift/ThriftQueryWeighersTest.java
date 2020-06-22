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
package com.palantir.atlasdb.keyvalue.cassandra.thrift;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.ThriftQueryWeighers.QueryWeigher;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.junit.Test;

public class ThriftQueryWeighersTest {

    private static final ByteBuffer BYTES1 = ByteBuffer.allocate(3);
    private static final ByteBuffer BYTES2 = ByteBuffer.allocate(7);
    private static final ColumnOrSuperColumn COLUMN_OR_SUPER = new ColumnOrSuperColumn();
    private static final KeySlice KEY_SLICE = new KeySlice();
    private static final Mutation MUTATION = new Mutation();

    private static final long TIME_TAKEN = 123L;

    private static final QueryWeight DEFAULT_WEIGHT = ImmutableQueryWeight.builder()
            .numBytes(ThriftQueryWeighers.ESTIMATED_NUM_BYTES_PER_ROW)
            .numDistinctRows(1)
            .timeTakenNanos(TIME_TAKEN)
            .build();

    @Test
    public void multigetSliceWeigherReturnsCorrectNumRows() {
        assertThatWeightSuccessReturnsCorrectNumberOfRows(ThriftQueryWeighers.multigetSlice(ImmutableList.of(BYTES1)));
    }

    @Test
    public void multigetSliceWeigherReturnsDefaultEstimateForFailure() {
        assertThatWeighFailureReturnsDefaultWeight(ThriftQueryWeighers.multigetSlice(ImmutableList.of(BYTES1)));
    }

    @Test
    public void rangeSlicesWeigherReturnsCorrectNumRows() {
        assertThatRangeSlicesWeigherReturnsCorrectNumRows(ThriftQueryWeighers.getRangeSlices(new KeyRange(1)));
    }

    @Test
    public void getRangeSlicesWeigherReturnsDefaultEstimateForFailure() {
        assertThatWeighFailureReturnsDefaultWeight(ThriftQueryWeighers.getRangeSlices(new KeyRange(1)));
    }

    @Test
    public void getWeigherReturnsCorrectNumRows() {
        assertThatGetWeighSuccessReturnsOneRow(ThriftQueryWeighers.GET);
    }

    @Test
    public void getWeigherReturnsDefaultEstimateForFailure() {
        assertThatWeighFailureReturnsDefaultWeight(ThriftQueryWeighers.GET);
    }

    @Test
    public void executeCql3QueryWeigherReturnsOneRow() {
        long actualNumRows = ThriftQueryWeighers.EXECUTE_CQL3_QUERY.weighSuccess(new CqlResult(),
                TIME_TAKEN).numDistinctRows();

        assertThat(actualNumRows).isEqualTo(1);
    }

    @Test
    public void cql3QueryWeigherReturnsDefaultEstimateForFailure() {
        assertThatWeighFailureReturnsDefaultWeight(ThriftQueryWeighers.EXECUTE_CQL3_QUERY);
    }

    @Test
    public void batchMutateWeigherReturnsCorrectNumRows() {
        Map<ByteBuffer, Map<String, List<Mutation>>> mutations = ImmutableMap.of(
                BYTES1, ImmutableMap.of(
                        "table1", ImmutableList.of(MUTATION, MUTATION),
                        "table2", ImmutableList.of(MUTATION)),
                BYTES2, ImmutableMap.of(
                        "table1", ImmutableList.of(MUTATION)));

        long actualNumRows = ThriftQueryWeighers.batchMutate(mutations).weighSuccess(null, TIME_TAKEN)
                .numDistinctRows();

        assertThat(actualNumRows).isEqualTo(2);
    }

    @Test
    public void batchMutateWeigherReturnsSameAsSuccessForFailure() {
        Map<ByteBuffer, Map<String, List<Mutation>>> mutations = ImmutableMap.of(
                BYTES1, ImmutableMap.of("foo", ImmutableList.of(MUTATION, MUTATION)));

        QueryWeigher<Void> weigher = ThriftQueryWeighers.batchMutate(mutations);

        QueryWeight expected = ThriftQueryWeighers.batchMutate(mutations).weighSuccess(null, TIME_TAKEN);

        QueryWeight actual = weigher.weighFailure(new RuntimeException(), TIME_TAKEN);

        assertThat(actual).isEqualTo(expected);
    }

    private void assertThatWeightSuccessReturnsCorrectNumberOfRows(
            QueryWeigher<Map<ByteBuffer, List<ColumnOrSuperColumn>>> mapQueryWeigher) {
        Map<ByteBuffer, List<ColumnOrSuperColumn>> result = ImmutableMap.of(
                BYTES1, ImmutableList.of(COLUMN_OR_SUPER, COLUMN_OR_SUPER),
                BYTES2, ImmutableList.of(COLUMN_OR_SUPER));

        long actualNumRows = mapQueryWeigher
                .weighSuccess(result, TIME_TAKEN)
                .numDistinctRows();

        assertThat(actualNumRows).isEqualTo(2);
    }

    private void assertThatWeighFailureReturnsDefaultWeight(QueryWeigher queryWeigher) {
        QueryWeight weight = queryWeigher.weighFailure(new RuntimeException(), TIME_TAKEN);
        assertThat(weight).isEqualTo(DEFAULT_WEIGHT);
    }

    private void assertThatRangeSlicesWeigherReturnsCorrectNumRows(QueryWeigher<List<KeySlice>> weigher) {
        List<KeySlice> result = ImmutableList.of(KEY_SLICE, KEY_SLICE, KEY_SLICE);

        long actualNumRows = weigher
                .weighSuccess(result, TIME_TAKEN)
                .numDistinctRows();

        assertThat(actualNumRows).isEqualTo(3);
    }

    private void assertThatGetWeighSuccessReturnsOneRow(QueryWeigher<ColumnOrSuperColumn> weighQuery) {
        long actualNumRows = weighQuery.weighSuccess(COLUMN_OR_SUPER, TIME_TAKEN).numDistinctRows();

        assertThat(actualNumRows).isEqualTo(1);
    }
}
