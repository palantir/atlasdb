/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Map;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Value;

class ValueExtractor extends ResultsExtractor<Map<Cell, Value>, Value> {

    static final Supplier<ResultsExtractor<Map<Cell, Value>, Value>> SUPPLIER =
            new Supplier<ResultsExtractor<Map<Cell, Value>, Value>>() {
        @Override
        public ResultsExtractor<Map<Cell, Value>, Value> get() {
            return new ValueExtractor(Maps.<Cell, Value>newHashMap());
        }
    };

    public ValueExtractor(Map<Cell, Value> collector) {
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
        if (!collector.containsKey(cell) && ts < startTs && selection.contains(cell.getColumnName())) {
            collector.put(cell, Value.create(val, ts));
        }
    }

    @Override
    public Map<Cell, Value> asMap() {
        return collector;
    }
}
