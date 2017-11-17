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

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ThriftQueryWeighersTest {

    private static final ByteBuffer BYTES1 = ByteBuffer.allocate(3);
    private static final ByteBuffer BYTES2 = ByteBuffer.allocate(7);
    private static final ColumnOrSuperColumn COLUMN_OR_SUPER = new ColumnOrSuperColumn();
    private static final Column COLUMN = new Column();
    private static final KeySlice KEY_SLICE = new KeySlice();
    private static final Mutation MUTATION = new Mutation();

    private static final long UNIMPORTANT_ARG = 123L;

    @Test
    public void multigetSliceWeigherReturnsCorrectNumRows() {
        Map<ByteBuffer, List<ColumnOrSuperColumn>> result = ImmutableMap.of(
                BYTES1, ImmutableList.of(COLUMN_OR_SUPER, COLUMN_OR_SUPER),
                BYTES2, ImmutableList.of(COLUMN_OR_SUPER));

        long actualNumRows = ThriftQueryWeighers.MULTIGET_SLICE.weigh(result, UNIMPORTANT_ARG).numDistinctRows();

        assertThat(actualNumRows).isEqualTo(2);
    }

    @Test
    public void rangeSlicesWeigherReturnsCorrectNumRows() {
        List<KeySlice> result = ImmutableList.of(KEY_SLICE, KEY_SLICE, KEY_SLICE);

        long actualNumRows = ThriftQueryWeighers.GET_RANGE_SLICES.weigh(result, UNIMPORTANT_ARG).numDistinctRows();

        assertThat(actualNumRows).isEqualTo(3);
    }

    @Test
    public void getWeigherReturnsCorrectNumRows() {
        long actualNumRows = ThriftQueryWeighers.GET.weigh(COLUMN_OR_SUPER, UNIMPORTANT_ARG).numDistinctRows();

        assertThat(actualNumRows).isEqualTo(1);
    }

    @Test
    public void executeCql3QueryWeigherReturnsOneRowAlways() {
        long actualNumRows = ThriftQueryWeighers.EXECUTE_CQL3_QUERY.weigh(new CqlResult(),
                UNIMPORTANT_ARG).numDistinctRows();

        assertThat(actualNumRows).isEqualTo(1);
    }

    @Test
    public void casQueryWeigherReturnsOneRowAlways() {
        long actualNumRows = ThriftQueryWeighers.cas(ImmutableList.of(COLUMN, COLUMN)).weigh(new CASResult(true),
                UNIMPORTANT_ARG).numDistinctRows();

        assertThat(actualNumRows).isEqualTo(1);
    }

    @Test
    public void batchMutateWeigherReturnsCorrectNumRows() {
        Map<ByteBuffer, Map<String, List<Mutation>>> mutations = ImmutableMap.of(
                BYTES1, ImmutableMap.of(
                        "foo", ImmutableList.of(MUTATION, MUTATION),
                        "bar", ImmutableList.of(MUTATION)),
                BYTES2, ImmutableMap.of(
                        "baz", ImmutableList.of(MUTATION)));

        long actualNumRows = ThriftQueryWeighers.batchMutate(mutations).weigh(null, UNIMPORTANT_ARG)
                .numDistinctRows();

        assertThat(actualNumRows).isEqualTo(3);
    }

}
