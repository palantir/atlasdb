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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.nexus.db.DBType;
import org.junit.Test;

public class RangePredicateHelperTest {
    private static final byte[] ROW_NAME = {1, 2, 3};
    private static final byte[] COL_NAME = {4, 5, 6};
    private static final long TS = 5L;

    @Test
    public void startRowInclusiveEmpty() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.ORACLE, builder).startRowInclusive(PtBytes.EMPTY_BYTE_ARRAY);
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEmpty();
        assertThat(query.getArgs()).isEmpty();
    }

    @Test
    public void startRowInclusiveForward() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.ORACLE, builder).startRowInclusive(ROW_NAME);
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEqualTo(" AND row_name >= ? ");
        assertThat(query.getArgs()).containsExactly(ROW_NAME);
    }

    @Test
    public void startRowInclusiveReverse() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(true, DBType.ORACLE, builder).startRowInclusive(ROW_NAME);
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEqualTo(" AND row_name <= ? ");
        assertThat(query.getArgs()).containsExactly(ROW_NAME);
    }

    @Test
    public void startCellInclusiveEmptyColName() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.ORACLE, builder)
                .startCellInclusive(ROW_NAME, PtBytes.EMPTY_BYTE_ARRAY);
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEqualTo(" AND row_name >= ? ");
        assertThat(query.getArgs()).containsExactly(ROW_NAME);
    }

    @Test
    public void startCellInclusiveForwardPostgres() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.POSTGRESQL, builder).startCellInclusive(ROW_NAME, COL_NAME);
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEqualTo(" AND (row_name, col_name) >= (?, ?)");
        assertThat(query.getArgs()).containsExactly(ROW_NAME, COL_NAME);
    }

    @Test
    public void startCellInclusiveReversePostgres() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(true, DBType.POSTGRESQL, builder).startCellInclusive(ROW_NAME, COL_NAME);
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEqualTo(" AND (row_name, col_name) <= (?, ?)");
        assertThat(query.getArgs()).containsExactly(ROW_NAME, COL_NAME);
    }

    @Test
    public void startCellInclusiveForwardOracle() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.ORACLE, builder).startCellInclusive(ROW_NAME, COL_NAME);
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEqualTo(" AND (row_name >= ? AND (row_name > ? OR col_name >= ?))");
        assertThat(query.getArgs()).containsExactly(ROW_NAME, ROW_NAME, COL_NAME);
    }

    @Test
    public void startCellInclusiveReverseOracle() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(true, DBType.ORACLE, builder).startCellInclusive(ROW_NAME, COL_NAME);
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEqualTo(" AND (row_name <= ? AND (row_name < ? OR col_name <= ?))");
        assertThat(query.getArgs()).containsExactly(ROW_NAME, ROW_NAME, COL_NAME);
    }

    @Test
    public void startCellTsInclusiveNullTimestamp() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.POSTGRESQL, builder).startCellTsInclusive(ROW_NAME, COL_NAME, null);
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEqualTo(" AND (row_name, col_name) >= (?, ?)");
        assertThat(query.getArgs()).containsExactly(ROW_NAME, COL_NAME);
    }

    @Test
    public void startCellTsInclusiveForwardPostgres() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.POSTGRESQL, builder).startCellTsInclusive(ROW_NAME, COL_NAME, TS);
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEqualTo(" AND (row_name, col_name, ts) >= (?, ?, ?)");
        assertThat(query.getArgs()).containsExactly(ROW_NAME, COL_NAME, TS);
    }

    @Test
    public void startCellTsInclusiveReversePostgres() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(true, DBType.POSTGRESQL, builder).startCellTsInclusive(ROW_NAME, COL_NAME, TS);
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEqualTo(" AND (row_name, col_name, ts) <= (?, ?, ?)");
        assertThat(query.getArgs()).containsExactly(ROW_NAME, COL_NAME, TS);
    }

    @Test
    public void startCellTsInclusiveForwardOracle() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.ORACLE, builder).startCellTsInclusive(ROW_NAME, COL_NAME, TS);
        FullQuery query = builder.build();

        assertThat(query.getQuery())
                .isEqualTo(" AND (row_name >= ? AND (row_name > ? OR col_name > ? OR (col_name = ? AND ts >= ?)))");
        assertThat(query.getArgs()).containsExactly(ROW_NAME, ROW_NAME, COL_NAME, COL_NAME, TS);
    }

    @Test
    public void startCellTsInclusiveReverseOracle() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(true, DBType.ORACLE, builder).startCellTsInclusive(ROW_NAME, COL_NAME, TS);
        FullQuery query = builder.build();

        assertThat(query.getQuery())
                .isEqualTo(" AND (row_name <= ? AND (row_name < ? OR col_name < ? OR (col_name = ? AND ts <= ?)))");
        assertThat(query.getArgs()).containsExactly(ROW_NAME, ROW_NAME, COL_NAME, COL_NAME, TS);
    }

    @Test
    public void endRowExclusiveEmpty() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.ORACLE, builder).endRowExclusive(PtBytes.EMPTY_BYTE_ARRAY);
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEmpty();
        assertThat(query.getArgs()).isEmpty();
    }

    @Test
    public void endRowExclusiveForward() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.ORACLE, builder).endRowExclusive(ROW_NAME);
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEqualTo(" AND row_name < ? ");
        assertThat(query.getArgs()).containsExactly(ROW_NAME);
    }

    @Test
    public void endRowExclusiveReverse() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(true, DBType.ORACLE, builder).endRowExclusive(ROW_NAME);
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEqualTo(" AND row_name > ? ");
        assertThat(query.getArgs()).containsExactly(ROW_NAME);
    }

    @Test
    public void columnSelectionEmpty() {
        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.ORACLE, builder).columnSelection(ImmutableList.of());
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEmpty();
        assertThat(query.getArgs()).isEmpty();
    }

    @Test
    public void columnSelection() {
        byte[] colTwo = new byte[] {7, 8, 9};

        FullQuery.Builder builder = FullQuery.builder();
        RangePredicateHelper.create(false, DBType.ORACLE, builder).columnSelection(ImmutableList.of(COL_NAME, colTwo));
        FullQuery query = builder.build();

        assertThat(query.getQuery()).isEqualTo(" AND (col_name = ? OR col_name = ?) ");
        assertThat(query.getArgs()).containsExactly(COL_NAME, colTwo);
    }
}
