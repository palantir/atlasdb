// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;

class TimestampExtractor extends ResultsExtractor<SetMultimap<Cell, Long>, Set<Long>> {

    static final Supplier<ResultsExtractor<SetMultimap<Cell, Long>, Set<Long>>> SUPPLIER =
            new Supplier<ResultsExtractor<SetMultimap<Cell, Long>, Set<Long>>>() {
        @Override
        public ResultsExtractor<SetMultimap<Cell, Long>, Set<Long>> get() {
            return new TimestampExtractor(HashMultimap.<Cell, Long>create());
        }
    };

    public TimestampExtractor(SetMultimap<Cell, Long> collector) {
        super(collector);
    }

    @Override
    public void internalExtractResult(long startTs,
                                      ColumnSelection selection,
                                      byte[] row,
                                      byte[] col,
                                      byte[] val,
                                      long ts) {
        Cell cell = Cell.create(row, col);
        if (ts < startTs && selection.contains(cell.getColumnName())) {
            collector.put(cell, ts);
        }
    }

    @Override
    public Map<Cell, Set<Long>> asMap() {
        return Multimaps.asMap(collector);
    }
}
