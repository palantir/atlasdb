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
package com.palantir.atlasdb;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.ThriftObjectSizeUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlResultType;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SuperColumn;
import org.junit.Test;

public class ThriftObjectSizeUtilsTest {
    private static final String TEST_NAME = "foo";
    private static final ByteBuffer TEST_NAME_BYTES = ByteBuffer.wrap(TEST_NAME.getBytes(StandardCharsets.UTF_8));
    private static final Column TEST_COLUMN = new Column(TEST_NAME_BYTES);

    private static final long TEST_NAME_SIZE = 3L;
    private static final long TEST_NAME_BYTES_SIZE = TEST_NAME_BYTES.remaining();
    private static final long TEST_COLUMN_SIZE = TEST_NAME_BYTES_SIZE + 4L + 4L + 8L;
    private static final ColumnOrSuperColumn EMPTY_COLUMN_OR_SUPERCOLUMN = new ColumnOrSuperColumn();
    public static final int NULL_SIZE = Integer.BYTES;
    private static final long EMPTY_COLUMN_OR_SUPERCOLUMN_SIZE = NULL_SIZE * 4;

    @Test
    public void returnEightForNullColumnOrSuperColumn() {
        assertThat(ThriftObjectSizeUtils.getColumnOrSuperColumnSize(null)).isEqualTo(NULL_SIZE);
    }

    @Test
    public void getSizeForEmptyColumnOrSuperColumn() {
        assertThat(ThriftObjectSizeUtils.getColumnOrSuperColumnSize(EMPTY_COLUMN_OR_SUPERCOLUMN))
                .isEqualTo(EMPTY_COLUMN_OR_SUPERCOLUMN_SIZE);
    }

    @Test
    public void getSizeForColumnOrSuperColumnWithAnEmptyColumn() {
        assertThat(ThriftObjectSizeUtils.getColumnOrSuperColumnSize(new ColumnOrSuperColumn().setColumn(new Column())))
                .isEqualTo(NULL_SIZE * 8);
    }

    @Test
    public void getSizeForColumnOrSuperColumnWithANonEmptyColumn() {
        assertThat(ThriftObjectSizeUtils.getColumnOrSuperColumnSize(new ColumnOrSuperColumn().setColumn(TEST_COLUMN)))
                .isEqualTo(NULL_SIZE * 3 + TEST_COLUMN_SIZE);
    }

    @Test
    public void getSizeForColumnOrSuperColumnWithANonEmptyColumnAndSuperColumn() {
        assertThat(ThriftObjectSizeUtils.getColumnOrSuperColumnSize(new ColumnOrSuperColumn()
                        .setColumn(TEST_COLUMN)
                        .setSuper_column(new SuperColumn(
                                ByteBuffer.wrap(TEST_NAME.getBytes(StandardCharsets.UTF_8)),
                                ImmutableList.of(TEST_COLUMN)))))
                .isEqualTo(NULL_SIZE * 2 + TEST_COLUMN_SIZE + TEST_NAME_BYTES_SIZE + TEST_COLUMN_SIZE);
    }

    @Test
    public void getSizeForNullByteBuffer() {
        assertThat(ThriftObjectSizeUtils.getByteBufferSize(null)).isEqualTo(NULL_SIZE);
    }

    @Test
    public void getSizeForEmptyByteBuffer() {
        assertThat(ThriftObjectSizeUtils.getByteBufferSize(ByteBuffer.wrap(new byte[] {})))
                .isEqualTo(0L);
    }

    @Test
    public void getSizeForNonEmptyByteBuffer() {
        assertThat(ThriftObjectSizeUtils.getByteBufferSize(ByteBuffer.wrap(TEST_NAME.getBytes(StandardCharsets.UTF_8))))
                .isEqualTo(TEST_NAME.getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    public void getSizeForNullCqlResult() {
        assertThat(ThriftObjectSizeUtils.getCqlResultSize(null)).isEqualTo(NULL_SIZE);
    }

    @Test
    public void getSizeForVoidCqlResult() {
        assertThat(ThriftObjectSizeUtils.getCqlResultSize(new CqlResult(CqlResultType.VOID)))
                .isEqualTo(NULL_SIZE * 4);
    }

    @Test
    public void getSizeForCqlResultWithRows() {
        assertThat(ThriftObjectSizeUtils.getCqlResultSize(
                        new CqlResult(CqlResultType.ROWS).setRows(ImmutableList.of(new CqlRow()))))
                .isEqualTo(NULL_SIZE * 5);
    }

    @Test
    public void getSizeForNullMutation() {
        assertThat(ThriftObjectSizeUtils.getMutationSize(null)).isEqualTo(NULL_SIZE);
    }

    @Test
    public void getSizeForEmptyMutation() {
        assertThat(ThriftObjectSizeUtils.getMutationSize(new Mutation())).isEqualTo(NULL_SIZE * 2);
    }

    @Test
    public void getSizeForMutationWithColumnOrSuperColumn() {
        assertThat(ThriftObjectSizeUtils.getMutationSize(
                        new Mutation().setColumn_or_supercolumn(EMPTY_COLUMN_OR_SUPERCOLUMN)))
                .isEqualTo(NULL_SIZE + EMPTY_COLUMN_OR_SUPERCOLUMN_SIZE);
    }

    @Test
    public void getSizeForMutationWithEmptyDeletion() {
        long emptyDeletionSize = Long.BYTES + 2 * NULL_SIZE;
        assertThat(ThriftObjectSizeUtils.getMutationSize(new Mutation().setDeletion(new Deletion())))
                .isEqualTo(NULL_SIZE + emptyDeletionSize);
    }

    @Test
    public void getSizeForMutationWithDeletionContainingSuperColumn() {
        long nonEmptyDeletionSize = Long.BYTES + TEST_NAME.getBytes(StandardCharsets.UTF_8).length + NULL_SIZE;
        assertThat(ThriftObjectSizeUtils.getMutationSize(new Mutation()
                        .setDeletion(new Deletion().setSuper_column(TEST_NAME.getBytes(StandardCharsets.UTF_8)))))
                .isEqualTo(NULL_SIZE + nonEmptyDeletionSize);
    }

    @Test
    public void getSizeForMutationWithDeletionContainingEmptySlicePredicate() {
        long deletionSize = Long.BYTES + NULL_SIZE + NULL_SIZE * 2;
        assertThat(ThriftObjectSizeUtils.getMutationSize(
                        new Mutation().setDeletion(new Deletion().setPredicate(new SlicePredicate()))))
                .isEqualTo(NULL_SIZE + deletionSize);
    }

    @Test
    public void getSizeForMutationWithDeletionContainingNonEmptySlicePredicate() {
        long deletionSize = Long.BYTES + NULL_SIZE + (TEST_NAME.getBytes(StandardCharsets.UTF_8).length + NULL_SIZE);
        assertThat(ThriftObjectSizeUtils.getMutationSize(new Mutation()
                        .setDeletion(new Deletion()
                                .setPredicate(new SlicePredicate()
                                        .setColumn_names(ImmutableList.of(
                                                ByteBuffer.wrap(TEST_NAME.getBytes(StandardCharsets.UTF_8))))))))
                .isEqualTo(NULL_SIZE + deletionSize);
    }

    @Test
    public void getSizeForMutationWithColumnOrSuperColumnAndDeletion() {
        long emptyDeletionSize = Long.BYTES + 2 * NULL_SIZE;
        assertThat(ThriftObjectSizeUtils.getMutationSize(new Mutation()
                        .setColumn_or_supercolumn(EMPTY_COLUMN_OR_SUPERCOLUMN)
                        .setDeletion(new Deletion())))
                .isEqualTo(EMPTY_COLUMN_OR_SUPERCOLUMN_SIZE + emptyDeletionSize);
    }

    @Test
    public void getSizeForNullKeySlice() {
        assertThat(ThriftObjectSizeUtils.getKeySliceSize(null)).isEqualTo(NULL_SIZE);
    }

    @Test
    public void getSizeForKeySliceWithKeyNotSetButColumnsSet() {
        assertThat(ThriftObjectSizeUtils.getKeySliceSize(
                        new KeySlice().setColumns(ImmutableList.of(EMPTY_COLUMN_OR_SUPERCOLUMN))))
                .isEqualTo(NULL_SIZE + EMPTY_COLUMN_OR_SUPERCOLUMN_SIZE);
    }

    @Test
    public void getSizeForKeySliceWithKeySetSetButColumnsNotSet() {
        assertThat(ThriftObjectSizeUtils.getKeySliceSize(
                        new KeySlice().setKey(TEST_NAME.getBytes(StandardCharsets.UTF_8))))
                .isEqualTo(NULL_SIZE + TEST_NAME.getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    public void getSizeForKeySliceWithKeyAndColumns() {
        assertThat(ThriftObjectSizeUtils.getKeySliceSize(new KeySlice()
                        .setKey(TEST_NAME.getBytes(StandardCharsets.UTF_8))
                        .setColumns(ImmutableList.of(EMPTY_COLUMN_OR_SUPERCOLUMN))))
                .isEqualTo(TEST_NAME.getBytes(StandardCharsets.UTF_8).length + EMPTY_COLUMN_OR_SUPERCOLUMN_SIZE);
    }

    @Test
    public void getSizeForBatchMutate() {
        Map<ByteBuffer, Map<String, List<Mutation>>> batchMutateMap = ImmutableMap.of(
                TEST_NAME_BYTES,
                ImmutableMap.of(
                        TEST_NAME,
                        ImmutableList.of(new Mutation().setColumn_or_supercolumn(EMPTY_COLUMN_OR_SUPERCOLUMN))));

        long expectedSize = TEST_NAME_BYTES_SIZE + TEST_NAME_SIZE + NULL_SIZE + EMPTY_COLUMN_OR_SUPERCOLUMN_SIZE;

        assertThat(ThriftObjectSizeUtils.getApproximateSizeOfMutationMap(batchMutateMap))
                .isEqualTo(expectedSize);
    }

    @Test
    public void getSizeOfMutationPerTableOnBatchMutate() {
        Map<ByteBuffer, Map<String, List<Mutation>>> batchMutateMap = ImmutableMap.of(
                TEST_NAME_BYTES,
                ImmutableMap.of(
                        TEST_NAME,
                        ImmutableList.of(new Mutation().setColumn_or_supercolumn(EMPTY_COLUMN_OR_SUPERCOLUMN))));

        long expectedSize = EMPTY_COLUMN_OR_SUPERCOLUMN_SIZE + NULL_SIZE + TEST_NAME_BYTES_SIZE;

        assertThat(ThriftObjectSizeUtils.getSizeOfMutationPerTable(batchMutateMap))
                .isEqualTo(ImmutableMap.of(TEST_NAME, expectedSize));
    }

    @Test
    public void getStringSize() {
        assertThat(ThriftObjectSizeUtils.getStringSize(TEST_NAME)).isEqualTo(TEST_NAME_SIZE);
    }

    @Test
    public void getMultigetResultSize() {
        Map<ByteBuffer, List<ColumnOrSuperColumn>> result =
                ImmutableMap.of(TEST_NAME_BYTES, ImmutableList.of(EMPTY_COLUMN_OR_SUPERCOLUMN));

        long expectedSize = TEST_NAME_BYTES_SIZE + EMPTY_COLUMN_OR_SUPERCOLUMN_SIZE;

        assertThat(ThriftObjectSizeUtils.getApproximateSizeOfColsByKey(result)).isEqualTo(expectedSize);
    }

    @Test
    public void getMultigetMultisliceResultSizeOneRowOneColumn() {
        Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> result =
                ImmutableMap.of(TEST_NAME_BYTES, ImmutableList.of(ImmutableList.of(EMPTY_COLUMN_OR_SUPERCOLUMN)));

        long expectedSize = TEST_NAME_BYTES_SIZE + EMPTY_COLUMN_OR_SUPERCOLUMN_SIZE;

        assertThat(ThriftObjectSizeUtils.getApproximateSizeOfColListsByKey(result))
                .isEqualTo(expectedSize);
    }

    @Test
    public void getMultigetMultisliceResultSizeMultipleColumnsOneQuery() {
        Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> result = ImmutableMap.of(
                TEST_NAME_BYTES,
                ImmutableList.of(ImmutableList.of(EMPTY_COLUMN_OR_SUPERCOLUMN, EMPTY_COLUMN_OR_SUPERCOLUMN)));

        long expectedSize = TEST_NAME_BYTES_SIZE + (2 * EMPTY_COLUMN_OR_SUPERCOLUMN_SIZE);

        assertThat(ThriftObjectSizeUtils.getApproximateSizeOfColListsByKey(result))
                .isEqualTo(expectedSize);
    }

    @Test
    public void getMultigetMultisliceResultSizeMultipleColumnsMultipleQueries() {
        Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> result = ImmutableMap.of(
                TEST_NAME_BYTES,
                ImmutableList.of(
                        ImmutableList.of(EMPTY_COLUMN_OR_SUPERCOLUMN),
                        ImmutableList.of(EMPTY_COLUMN_OR_SUPERCOLUMN, EMPTY_COLUMN_OR_SUPERCOLUMN)));

        long expectedSize = TEST_NAME_BYTES_SIZE + (3 * EMPTY_COLUMN_OR_SUPERCOLUMN_SIZE);

        assertThat(ThriftObjectSizeUtils.getApproximateSizeOfColListsByKey(result))
                .isEqualTo(expectedSize);
    }

    @Test
    public void getKeySlicesSize() {
        List<KeySlice> slices = ImmutableList.of(
                new KeySlice().setKey(TEST_NAME_BYTES).setColumns(ImmutableList.of(EMPTY_COLUMN_OR_SUPERCOLUMN)));

        long expectedSize = TEST_NAME_BYTES_SIZE + EMPTY_COLUMN_OR_SUPERCOLUMN_SIZE;

        assertThat(ThriftObjectSizeUtils.getApproximateSizeOfKeySlices(slices)).isEqualTo(expectedSize);
    }

    @Test
    public void getCasSize() {
        List<Column> columns = ImmutableList.of(TEST_COLUMN, TEST_COLUMN);

        long expectedSize = TEST_COLUMN_SIZE * 2;

        assertThat(ThriftObjectSizeUtils.getCasByteCount(columns)).isEqualTo(expectedSize);
    }
}
