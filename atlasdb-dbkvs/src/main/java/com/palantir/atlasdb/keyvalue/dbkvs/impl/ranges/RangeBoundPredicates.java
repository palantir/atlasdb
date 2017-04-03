/**
 * Copyright 2017 Palantir Technologies
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

public class RangeBoundPredicates {
    public final String predicates;
    public final List<Object> args;

    private RangeBoundPredicates(String predicates, List<Object> args) {
        this.predicates = predicates;
        this.args = args;
    }

    public static Builder builder(boolean reverseRange) {
        return new Builder(reverseRange);
    }

    public static class Builder {
        private final StringBuilder predicates = new StringBuilder(100);
        private final List<Object> args = new ArrayList<>(10);
        private final boolean reverse;

        public Builder(boolean reverse) {
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
                // Warning: this syntax is not supported by Oracle
                predicates.append(reverse ? " AND (row_name, col_name) <= (?, ?) " : " AND (row_name, col_name) >= (?, ?) ");
                args.add(startRowInclusive);
                args.add(startColumnInclusive);
            } else {
                startRowInclusive(startRowInclusive);
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
