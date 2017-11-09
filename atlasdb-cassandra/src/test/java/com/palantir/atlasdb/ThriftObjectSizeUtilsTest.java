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

package com.palantir.atlasdb;

import static org.assertj.core.api.Java6Assertions.assertThat;

import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlResultType;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.SuperColumn;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.cassandra.ThriftObjectSizeUtils;

public class ThriftObjectSizeUtilsTest {

    private static final String TEST_MAME = "test";
    private static final Column TEST_COLUMN = new Column(ByteBuffer.wrap(TEST_MAME.getBytes()));


    private static final long TEST_COLUMN_SIZE = 4L + TEST_MAME.getBytes().length + 4L + 8L;


    @Test
    public void returnEightForNullColumnOrSuperColumn() {
        assertThat(ThriftObjectSizeUtils.getColumnOrSuperColumnSize(null)).isEqualTo(Integer.BYTES);
    }

    @Test
    public void getSizeForEmptyColumnOrSuperColumn() {
        assertThat(ThriftObjectSizeUtils.getColumnOrSuperColumnSize(new ColumnOrSuperColumn())).isEqualTo(
                Integer.BYTES * 4);
    }

    @Test
    public void getSizeForColumnOrSuperColumnWithAnEmptyColumn() {
        ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
        columnOrSuperColumn.setColumn(new Column());
        assertThat(ThriftObjectSizeUtils.getColumnOrSuperColumnSize(columnOrSuperColumn)).isEqualTo(
                Integer.BYTES * 8);
    }

    @Test
    public void getSizeForColumnOrSuperColumnWithANonEmptyColumn() {
        assertThat(ThriftObjectSizeUtils.getColumnOrSuperColumnSize(new ColumnOrSuperColumn().setColumn(TEST_COLUMN)))
                .isEqualTo(Integer.BYTES * 3 + TEST_COLUMN_SIZE);
    }

    @Test
    public void getSizeForColumnOrSuperColumnWithANonEmptyColumnAndSuperColumn() {
        ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
        columnOrSuperColumn.setColumn(TEST_COLUMN);
        columnOrSuperColumn.setSuper_column(new SuperColumn(ByteBuffer.wrap(TEST_MAME.getBytes()), ImmutableList.of(TEST_COLUMN)));
        assertThat(ThriftObjectSizeUtils.getColumnOrSuperColumnSize(columnOrSuperColumn)).isEqualTo(
                Integer.BYTES * 2 + TEST_COLUMN_SIZE + TEST_MAME.getBytes().length + TEST_COLUMN_SIZE);
    }

    @Test
    public void getSizeForNullCqlResult() {
        assertThat(ThriftObjectSizeUtils.getCqlResultSize(null)).isEqualTo(Integer.BYTES);
    }

    @Test
    public void getSizeForVoidCqlResult() {
        assertThat(ThriftObjectSizeUtils.getCqlResultSize(new CqlResult(CqlResultType.VOID)))
                .isEqualTo(Integer.BYTES * 4);
    }

    @Test
    public void getSizeForCqlResultWithRows() {
        assertThat(ThriftObjectSizeUtils.getCqlResultSize(
                new CqlResult(CqlResultType.ROWS).setRows(ImmutableList.of(new CqlRow()))))
                .isEqualTo(Integer.BYTES * 5);
    }
}
