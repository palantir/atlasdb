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

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.nexus.db.DBType;
import java.util.Collection;
import javax.annotation.Nullable;

public final class RangePredicateHelper {
    private final boolean reverse;
    private final TupleComparisonStrategy tupleComparisonStrategy;
    private final FullQuery.Builder queryBuilder;

    private RangePredicateHelper(
            boolean reverse, TupleComparisonStrategy tupleComparisonStrategy, FullQuery.Builder queryBuilder) {
        this.reverse = reverse;
        this.tupleComparisonStrategy = tupleComparisonStrategy;
        this.queryBuilder = queryBuilder;
    }

    public static RangePredicateHelper create(boolean reverse, DBType dbType, FullQuery.Builder builder) {
        TupleComparisonStrategy tupleComparisonStrategy = getTupleComparisonStrategyByDbType(dbType);
        return new RangePredicateHelper(reverse, tupleComparisonStrategy, builder);
    }

    public RangePredicateHelper startRowInclusive(byte[] startRow) {
        if (startRow.length > 0) {
            queryBuilder.append(reverse ? " AND row_name <= ? " : " AND row_name >= ? ", startRow);
        }
        return this;
    }

    public RangePredicateHelper startCellInclusive(byte[] startRow, byte[] startCol) {
        if (startCol.length > 0) {
            queryBuilder.append(" AND ");
            if (reverse) {
                tupleComparisonStrategy.cellLessOrEqualTo(startRow, startCol, queryBuilder);
            } else {
                tupleComparisonStrategy.cellGreaterOrEqualTo(startRow, startCol, queryBuilder);
            }
        } else {
            startRowInclusive(startRow);
        }
        return this;
    }

    public RangePredicateHelper startCellTsInclusive(byte[] startRow, byte[] startCol, @Nullable Long startTs) {
        if (startTs != null) {
            queryBuilder.append(" AND ");
            if (reverse) {
                tupleComparisonStrategy.cellTsLessOrEqualTo(startRow, startCol, startTs, queryBuilder);
            } else {
                tupleComparisonStrategy.cellTsGreaterOrEqualTo(startRow, startCol, startTs, queryBuilder);
            }
        } else {
            startCellInclusive(startRow, startCol);
        }
        return this;
    }

    public RangePredicateHelper endRowExclusive(byte[] endExclusive) {
        if (endExclusive.length > 0) {
            queryBuilder.append(reverse ? " AND row_name > ? " : " AND row_name < ? ", endExclusive);
        }
        return this;
    }

    public RangePredicateHelper columnSelection(Collection<byte[]> columns) {
        if (!columns.isEmpty()) {
            Iterable<String> colnameConditions = Iterables.limit(Iterables.cycle("col_name = ?"), columns.size());
            queryBuilder.append(" AND (" + Joiner.on(" OR ").join(colnameConditions) + ") ");
            queryBuilder.addAllArgs(columns);
        }
        return this;
    }

    private static TupleComparisonStrategy getTupleComparisonStrategyByDbType(DBType dbType) {
        if (dbType == DBType.ORACLE) {
            return TupleComparisonStrategy.WITHOUT_ROW_VALUE_SYNTAX;
        } else {
            return TupleComparisonStrategy.USING_ROW_VALUE_SYNTAX;
        }
    }

    private enum TupleComparisonStrategy {
        USING_ROW_VALUE_SYNTAX {
            @Override
            void cellGreaterOrEqualTo(byte[] rhsRow, byte[] rhsCol, FullQuery.Builder builder) {
                builder.append("(row_name, col_name) >= (?, ?)", rhsRow, rhsCol);
            }

            @Override
            void cellLessOrEqualTo(byte[] rhsRow, byte[] rhsCol, FullQuery.Builder builder) {
                builder.append("(row_name, col_name) <= (?, ?)", rhsRow, rhsCol);
            }

            @Override
            void cellTsGreaterOrEqualTo(byte[] rhsRow, byte[] rhsCol, long ts, FullQuery.Builder builder) {
                builder.append("(row_name, col_name, ts) >= (?, ?, ?)", rhsRow, rhsCol, ts);
            }

            @Override
            void cellTsLessOrEqualTo(byte[] rhsRow, byte[] rhsCol, long ts, FullQuery.Builder builder) {
                builder.append("(row_name, col_name, ts) <= (?, ?, ?)", rhsRow, rhsCol, ts);
            }
        },
        WITHOUT_ROW_VALUE_SYNTAX {
            @Override
            void cellGreaterOrEqualTo(byte[] rhsRow, byte[] rhsCol, FullQuery.Builder builder) {
                builder.append("(row_name >= ? AND (row_name > ? OR col_name >= ?))", rhsRow, rhsRow, rhsCol);
            }

            @Override
            void cellLessOrEqualTo(byte[] rhsRow, byte[] rhsCol, FullQuery.Builder builder) {
                builder.append("(row_name <= ? AND (row_name < ? OR col_name <= ?))", rhsRow, rhsRow, rhsCol);
            }

            @Override
            void cellTsGreaterOrEqualTo(byte[] rhsRow, byte[] rhsCol, long ts, FullQuery.Builder builder) {
                builder.append("(row_name >= ? AND (row_name > ? OR col_name > ? OR (col_name = ? AND ts >= ?)))")
                        .addArg(rhsRow)
                        .addArg(rhsRow)
                        .addArg(rhsCol)
                        .addArg(rhsCol)
                        .addArg(ts);
            }

            @Override
            void cellTsLessOrEqualTo(byte[] rhsRow, byte[] rhsCol, long ts, FullQuery.Builder builder) {
                builder.append("(row_name <= ? AND (row_name < ? OR col_name < ? OR (col_name = ? AND ts <= ?)))")
                        .addArg(rhsRow)
                        .addArg(rhsRow)
                        .addArg(rhsCol)
                        .addArg(rhsCol)
                        .addArg(ts);
            }
        };

        abstract void cellGreaterOrEqualTo(byte[] rhsRow, byte[] rhsCol, FullQuery.Builder builder);

        abstract void cellLessOrEqualTo(byte[] rhsRow, byte[] rhsCol, FullQuery.Builder builder);

        abstract void cellTsGreaterOrEqualTo(byte[] rhsRow, byte[] rhsCol, long rhsTs, FullQuery.Builder builder);

        abstract void cellTsLessOrEqualTo(byte[] rhsRow, byte[] rhsCol, long rhsTs, FullQuery.Builder builder);
    }
}
