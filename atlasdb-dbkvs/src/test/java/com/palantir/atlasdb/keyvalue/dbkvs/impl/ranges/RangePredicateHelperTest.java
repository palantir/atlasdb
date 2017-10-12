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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.nexus.db.DBType;

public class RangePredicateHelperTest {

    @Test
    public void startRowInclusiveEmpty() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.ORACLE, builder).startRowInclusive(PtBytes.EMPTY_BYTE_ARRAY);
        FullQuery query = builder.build();
        assertThat(query.getQuery(), isEmptyString());
        assertThat(query.getArgs(), emptyArray());
    }

    @Test
    public void startRowInclusiveForward() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] rowName = new byte[] { 1, 2, 3 };
        RangePredicateHelper.create(false, DBType.ORACLE, builder).startRowInclusive(rowName);
        FullQuery query = builder.build();
        assertThat(query.getQuery(), equalTo(" AND row_name >= ? "));
        assertThat(query.getArgs(), arrayContaining((Object) rowName));
    }

    @Test
    public void startRowInclusiveReverse() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] rowName = new byte[] { 1, 2, 3 };
        RangePredicateHelper.create(true, DBType.ORACLE, builder).startRowInclusive(rowName);
        FullQuery query = builder.build();
        assertThat(query.getQuery(), equalTo(" AND row_name <= ? "));
        assertThat(query.getArgs(), arrayContaining((Object) rowName));
    }

    @Test
    public void startCellInclusiveEmptyColName() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] rowName = new byte[] { 1, 2, 3 };
        RangePredicateHelper.create(false, DBType.ORACLE, builder)
                .startCellInclusive(rowName, PtBytes.EMPTY_BYTE_ARRAY);
        FullQuery query = builder.build();
        assertThat(query.getQuery(), equalTo(" AND row_name >= ? "));
        assertThat(query.getArgs(), arrayContaining((Object) rowName));
    }

    @Test
    public void startCellInclusiveForwardPostgres() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] rowName = new byte[] { 1, 2, 3 };
        byte[] colName = new byte[] { 4, 5, 6 };
        RangePredicateHelper.create(false, DBType.POSTGRESQL, builder).startCellInclusive(rowName, colName);
        FullQuery query = builder.build();
        assertThat(query.getQuery(), equalTo(" AND (row_name, col_name) >= (?, ?)"));
        assertThat(query.getArgs(), arrayContaining(rowName, colName));
    }

    @Test
    public void startCellInclusiveReversePostgres() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] rowName = new byte[] { 1, 2, 3 };
        byte[] colName = new byte[] { 4, 5, 6 };
        RangePredicateHelper.create(true, DBType.POSTGRESQL, builder).startCellInclusive(rowName, colName);
        FullQuery query = builder.build();
        assertThat(query.getQuery(), equalTo(" AND (row_name, col_name) <= (?, ?)"));
        assertThat(query.getArgs(), arrayContaining(rowName, colName));
    }

    @Test
    public void startCellInclusiveForwardOracle() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] rowName = new byte[] { 1, 2, 3 };
        byte[] colName = new byte[] { 4, 5, 6 };
        RangePredicateHelper.create(false, DBType.ORACLE, builder).startCellInclusive(rowName, colName);
        FullQuery query = builder.build();
        assertThat(query.getQuery(), equalTo(" AND (row_name >= ? AND (row_name > ? OR col_name >= ?))"));
        assertThat(query.getArgs(), arrayContaining(rowName, rowName, colName));
    }

    @Test
    public void startCellInclusiveReverseOracle() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] rowName = new byte[] { 1, 2, 3 };
        byte[] colName = new byte[] { 4, 5, 6 };
        RangePredicateHelper.create(true, DBType.ORACLE, builder).startCellInclusive(rowName, colName);
        FullQuery query = builder.build();
        assertThat(query.getQuery(), equalTo(" AND (row_name <= ? AND (row_name < ? OR col_name <= ?))"));
        assertThat(query.getArgs(), arrayContaining(rowName, rowName, colName));
    }

    @Test
    public void startCellTsInclusiveNullTimestamp() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] rowName = new byte[] { 1, 2, 3 };
        byte[] colName = new byte[] { 4, 5, 6 };
        RangePredicateHelper.create(false, DBType.POSTGRESQL, builder).startCellTsInclusive(rowName, colName, null);
        FullQuery query = builder.build();
        assertThat(query.getQuery(), equalTo(" AND (row_name, col_name) >= (?, ?)"));
        assertThat(query.getArgs(), arrayContaining(rowName, colName));
    }

    @Test
    public void startCellTsInclusiveForwardPostgres() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] rowName = new byte[] { 1, 2, 3 };
        byte[] colName = new byte[] { 4, 5, 6 };
        RangePredicateHelper.create(false, DBType.POSTGRESQL, builder).startCellTsInclusive(rowName, colName, 5L);
        FullQuery query = builder.build();
        assertThat(query.getQuery(), equalTo(" AND (row_name, col_name, ts) >= (?, ?, ?)"));
        assertThat(query.getArgs(), arrayContaining(rowName, colName, 5L));
    }

    @Test
    public void startCellTsInclusiveReversePostgres() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] rowName = new byte[] { 1, 2, 3 };
        byte[] colName = new byte[] { 4, 5, 6 };
        RangePredicateHelper.create(true, DBType.POSTGRESQL, builder).startCellTsInclusive(rowName, colName, 5L);
        FullQuery query = builder.build();
        assertThat(query.getQuery(), equalTo(" AND (row_name, col_name, ts) <= (?, ?, ?)"));
        assertThat(query.getArgs(), arrayContaining(rowName, colName, 5L));
    }

    @Test
    public void startCellTsInclusiveForwardOracle() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] rowName = new byte[] { 1, 2, 3 };
        byte[] colName = new byte[] { 4, 5, 6 };
        RangePredicateHelper.create(false, DBType.ORACLE, builder).startCellTsInclusive(rowName, colName, 5L);
        FullQuery query = builder.build();
        assertThat(query.getQuery(),
                equalTo(" AND (row_name >= ? AND (row_name > ? OR col_name > ? OR (col_name = ? AND ts >= ?)))"));
        assertThat(query.getArgs(), arrayContaining(rowName, rowName, colName, colName, 5L));
    }

    @Test
    public void startCellTsInclusiveReverseOracle() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] rowName = new byte[] { 1, 2, 3 };
        byte[] colName = new byte[] { 4, 5, 6 };
        RangePredicateHelper.create(true, DBType.ORACLE, builder).startCellTsInclusive(rowName, colName, 5L);
        FullQuery query = builder.build();
        assertThat(query.getQuery(),
                equalTo(" AND (row_name <= ? AND (row_name < ? OR col_name < ? OR (col_name = ? AND ts <= ?)))"));
        assertThat(query.getArgs(), arrayContaining(rowName, rowName, colName, colName, 5L));
    }

    @Test
    public void endRowExclusiveEmpty() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.ORACLE, builder).endRowExclusive(PtBytes.EMPTY_BYTE_ARRAY);
        FullQuery query = builder.build();
        assertThat(query.getQuery(), isEmptyString());
        assertThat(query.getArgs(), emptyArray());
    }

    @Test
    public void endRowExclusiveForward() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] rowName = new byte[] { 1, 2, 3 };
        RangePredicateHelper.create(false, DBType.ORACLE, builder).endRowExclusive(rowName);
        FullQuery query = builder.build();
        assertThat(query.getQuery(), equalTo(" AND row_name < ? "));
        assertThat(query.getArgs(), arrayContaining((Object) rowName));
    }

    @Test
    public void endRowExclusiveReverse() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] rowName = new byte[] { 1, 2, 3 };
        RangePredicateHelper.create(true, DBType.ORACLE, builder).endRowExclusive(rowName);
        FullQuery query = builder.build();
        assertThat(query.getQuery(), equalTo(" AND row_name > ? "));
        assertThat(query.getArgs(), arrayContaining((Object) rowName));
    }

    @Test
    public void columnSelectionEmpty() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.ORACLE, builder).columnSelection(ImmutableList.of());
        FullQuery query = builder.build();
        assertThat(query.getQuery(), isEmptyString());
        assertThat(query.getArgs(), emptyArray());
    }

    @Test
    public void columnSelection() {
        FullQuery.Builder builder = FullQuery.builder();
        byte[] colOne = new byte[] { 1, 2, 3 };
        byte[] colTwo = new byte[] { 4, 5, 6 };
        RangePredicateHelper.create(false, DBType.ORACLE, builder).columnSelection(ImmutableList.of(colOne, colTwo));
        FullQuery query = builder.build();
        assertThat(query.getQuery(), equalTo(" AND (col_name = ? OR col_name = ?) "));
        assertThat(query.getArgs(), arrayContaining(colOne, colTwo));
    }

}
