/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.qos;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.qos.ImmutableQueryWeight;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.qos.QueryWeight;

public class ThriftQueryWeighersTest {

    private static final ByteBuffer BYTES1 = ByteBuffer.allocate(3);
    private static final ByteBuffer BYTES2 = ByteBuffer.allocate(7);
    private static final ColumnOrSuperColumn COLUMN_OR_SUPER = new ColumnOrSuperColumn();
    private static final KeySlice KEY_SLICE = new KeySlice();
    private static final Mutation MUTATION = new Mutation();

    private static final long TIME_TAKEN = 123L;

    private static final QueryWeight DEFAULT_WEIGHT = ImmutableQueryWeight.builder()
            .from(ThriftQueryWeighers.DEFAULT_ESTIMATED_WEIGHT)
            .timeTakenNanos(TIME_TAKEN)
            .build();

    @Test
    public void multigetSliceWeigherEstimatesNumberOfBytesBasedOnNumberOfRows() {
        assertThatEstimatesAreCorrect(false,
                ThriftQueryWeighers.multigetSlice(ImmutableList.of(BYTES1), false),
                ThriftQueryWeighers.multigetSlice(ImmutableList.of(BYTES1, BYTES2), false));
    }

    @Test
    public void multigetSliceWithZeroEstimateWeigherEstimatesZeroNumberOfBytes() {
        assertThatEstimatesAreCorrect(true,
                ThriftQueryWeighers.multigetSlice(ImmutableList.of(BYTES1), true),
                ThriftQueryWeighers.multigetSlice(ImmutableList.of(BYTES1, BYTES2), true));
    }

    @Test
    public void multigetSliceWeigherReturnsCorrectNumRows() {
        assertThatWeightSuccessReturnsCorrectNumberOfRows(
                ThriftQueryWeighers.multigetSlice(ImmutableList.of(BYTES1), false));
        assertThatWeightSuccessReturnsCorrectNumberOfRows(
                ThriftQueryWeighers.multigetSlice(ImmutableList.of(BYTES1), true));
    }

    private void assertThatWeightSuccessReturnsCorrectNumberOfRows(
            QosClient.QueryWeigher<Map<ByteBuffer, List<ColumnOrSuperColumn>>> mapQueryWeigher) {
        Map<ByteBuffer, List<ColumnOrSuperColumn>> result = ImmutableMap.of(
                BYTES1, ImmutableList.of(COLUMN_OR_SUPER, COLUMN_OR_SUPER),
                BYTES2, ImmutableList.of(COLUMN_OR_SUPER));

        long actualNumRows = mapQueryWeigher
                .weighSuccess(result, TIME_TAKEN)
                .numDistinctRows();

        assertThat(actualNumRows).isEqualTo(2);
    }

    @Test
    public void rangeSlicesWeigherEstimatesNumberOfBytesBasedOnNumberOfRows() {
        assertThatEstimatesAreCorrect(false,
                ThriftQueryWeighers.getRangeSlices(new KeyRange(1), false),
                ThriftQueryWeighers.getRangeSlices(new KeyRange(2), false));
    }

    @Test
    public void rangeSlicesWeigherWithZeroEstimateEstimatesZeroNumberOfBytes() {
        assertThatEstimatesAreCorrect(true,
                ThriftQueryWeighers.getRangeSlices(new KeyRange(1), true),
                ThriftQueryWeighers.getRangeSlices(new KeyRange(2), true));
    }

    private void assertThatEstimatesAreCorrect(boolean zeroEstimate,
            QosClient.QueryWeigher queryWeigherOneRow,
            QosClient.QueryWeigher queryWeigherTwoRows) {
        long numBytesWithOneRow = queryWeigherOneRow.estimate().numBytes();
        long numBytesWithTwoRows = queryWeigherTwoRows.estimate().numBytes();

        if (zeroEstimate) {
            assertThat(numBytesWithOneRow).isEqualTo(0L);
        } else {
            assertThat(numBytesWithOneRow).isGreaterThan(0L);
        }
        assertThat(numBytesWithTwoRows).isEqualTo(zeroEstimate ? 0L : numBytesWithOneRow * 2);
    }

    @Test
    public void rangeSlicesWeigherReturnsCorrectNumRows() {
        assertThatRangeSlicesWeigherReturnsCorrectNumRows(ThriftQueryWeighers.getRangeSlices(new KeyRange(1), false));
        assertThatRangeSlicesWeigherReturnsCorrectNumRows(ThriftQueryWeighers.getRangeSlices(new KeyRange(1), true));
    }

    private void assertThatRangeSlicesWeigherReturnsCorrectNumRows(
            QosClient.QueryWeigher<List<KeySlice>> weigher) {
        List<KeySlice> result = ImmutableList.of(KEY_SLICE, KEY_SLICE, KEY_SLICE);

        long actualNumRows = weigher
                .weighSuccess(result, TIME_TAKEN)
                .numDistinctRows();

        assertThat(actualNumRows).isEqualTo(3);
    }

    @Test
    public void getWeigherReturnsCorrectNumRows() {
        assertThatGetWeighSuccessReturnsOneRow(ThriftQueryWeighers.get(false));
        assertThatGetWeighSuccessReturnsOneRow(ThriftQueryWeighers.get(true));
    }

    private void assertThatGetWeighSuccessReturnsOneRow(
            QosClient.QueryWeigher<ColumnOrSuperColumn> weighQuery) {
        long actualNumRows = weighQuery.weighSuccess(COLUMN_OR_SUPER, TIME_TAKEN).numDistinctRows();

        assertThat(actualNumRows).isEqualTo(1);
    }

    @Test
    public void executeCql3QueryWeigherReturnsOneRowAlways() {
        long actualNumRows = ThriftQueryWeighers.EXECUTE_CQL3_QUERY.weighSuccess(new CqlResult(),
                TIME_TAKEN).numDistinctRows();

        assertThat(actualNumRows).isEqualTo(1);
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
    public void multigetSliceWeigherReturnsDefaultEstimateForFailure() {
        assertThatWeighFailureReturnsDefaultWeight(ThriftQueryWeighers.multigetSlice(ImmutableList.of(BYTES1), false));
        assertThatWeighFailureReturnsDefaultWeight(ThriftQueryWeighers.multigetSlice(ImmutableList.of(BYTES1), true));
    }

    @Test
    public void getWeigherReturnsDefaultEstimateForFailure() {
        assertThatWeighFailureReturnsDefaultWeight(ThriftQueryWeighers.get(false));
        assertThatWeighFailureReturnsDefaultWeight(ThriftQueryWeighers.get(true));
    }

    @Test
    public void getRangeSlicesWeigherReturnsDefaultEstimateForFailure() {
        assertThatWeighFailureReturnsDefaultWeight(ThriftQueryWeighers.getRangeSlices(new KeyRange(1), false));
        assertThatWeighFailureReturnsDefaultWeight(ThriftQueryWeighers.getRangeSlices(new KeyRange(1), true));
    }

    @Test
    public void batchMutateWeigherReturnsEstimateForFailure() {
        Map<ByteBuffer, Map<String, List<Mutation>>> mutations = ImmutableMap.of(
                BYTES1, ImmutableMap.of("foo", ImmutableList.of(MUTATION, MUTATION)));

        QosClient.QueryWeigher<Void> weigher = ThriftQueryWeighers.batchMutate(mutations);

        QueryWeight expected = ImmutableQueryWeight.builder()
                .from(weigher.estimate())
                .timeTakenNanos(TIME_TAKEN)
                .build();

        QueryWeight actual = weigher.weighFailure(new RuntimeException(), TIME_TAKEN);

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void cql3QueryWeigherReturnsDefaultEstimateForFailure() {
        assertThatWeighFailureReturnsDefaultWeight(ThriftQueryWeighers.EXECUTE_CQL3_QUERY);
    }

    private void assertThatWeighFailureReturnsDefaultWeight(
            QosClient.QueryWeigher queryWeigher) {
        QueryWeight weight = queryWeigher.weighFailure(new RuntimeException(), TIME_TAKEN);

        assertThat(weight).isEqualTo(DEFAULT_WEIGHT);
    }
}
