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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.nexus.db.DBType;

public final class RangeBoundPredicates {
    public final String predicates;
    public final List<Object> args;

    private RangeBoundPredicates(String predicates, List<Object> args) {
        this.predicates = predicates;
        this.args = args;
    }

    public static Builder builder(DBType dbType, boolean reverseRange) {
        return new Builder(dbType, reverseRange);
    }

    public FullQuery asFullQuery() {
        return new FullQuery(predicates).withArgs(args);
    }

    public static class Builder {
        private final StringBuilder predicates = new StringBuilder(100);
        private final List<Object> args = new ArrayList<>(10);
        private final DBType dbType;
        private final boolean reverse;

        public Builder(DBType dbType, boolean reverse) {
            this.dbType = dbType;
            this.reverse = reverse;
        }

        public Builder startRowInclusive(byte[] startInclusive) {
            if (startInclusive.length > 0) {
                predicates.append(reverse ? " AND row_name <= ? " : " AND row_name >= ? ");
                args.add(startInclusive);
            }
            return this;
        }

        public Builder startCellInclusive(byte[] startRowInclusive, byte[] startColumnInclusive) {
            if (startColumnInclusive.length > 0) {
                Preconditions.checkArgument(startRowInclusive.length > 0);
                if (dbType == DBType.ORACLE) {
                    predicates.append(reverse
                            ? "AND row_name <= ? AND (row_name < ? OR col_name <= ?)"
                            : "AND row_name >= ? AND (row_name > ? OR col_name >= ?)");
                    args.add(startRowInclusive);
                    args.add(startRowInclusive);
                    args.add(startColumnInclusive);
                } else {
                    predicates.append(reverse
                            ? " AND (row_name, col_name) <= (?, ?) "
                            : " AND (row_name, col_name) >= (?, ?) ");
                    args.add(startRowInclusive);
                    args.add(startColumnInclusive);
                }
            } else {
                startRowInclusive(startRowInclusive);
            }
            return this;
        }

        public Builder startCellTsInclusive(byte[] startRowInclusive,
                                            byte[] startColumnInclusive,
                                            @Nullable Long startTsInclusive) {
            if (startTsInclusive != null) {
                Preconditions.checkArgument(startRowInclusive.length > 0);
                Preconditions.checkArgument(startColumnInclusive.length > 0);
                if (dbType == DBType.ORACLE) {
                    predicates.append(reverse
                            ? " AND row_name <= ? AND (row_name < ? OR col_name < ? OR (col_name = ? AND ts <= ?))"
                            : " AND row_name >= ? AND (row_name > ? OR col_name > ? OR (col_name = ? AND ts >= ?))");
                    args.add(startRowInclusive);
                    args.add(startRowInclusive);
                    args.add(startColumnInclusive);
                    args.add(startColumnInclusive);
                    args.add(startTsInclusive);
                } else {
                    predicates.append(reverse
                            ? " AND (row_name, col_name, ts) <= (?, ?, ?) "
                            : " AND (row_name, col_name, ts) >= (?, ?, ?) ");
                    args.add(startRowInclusive);
                    args.add(startColumnInclusive);
                    args.add(startTsInclusive);
                }
            } else {
                startCellInclusive(startRowInclusive, startColumnInclusive);
            }
            return this;
        }

        public Builder endRowExclusive(byte[] endExclusive) {
            if (endExclusive.length > 0) {
                predicates.append(reverse ? " AND row_name > ? " : " AND row_name < ? ");
                args.add(endExclusive);
            }
            return this;
        }

        public Builder columnSelection(Collection<byte[]> columns) {
            if (!columns.isEmpty()) {
                Iterable colnameConditions = Iterables.limit(Iterables.cycle("col_name = ?"), columns.size());
                predicates.append(" AND (" + Joiner.on(" OR ").join(colnameConditions) + ") ");
                args.addAll(columns);
            }
            return this;
        }

        public RangeBoundPredicates build() {
            return new RangeBoundPredicates(predicates.toString(), args);
        }
    }
}
