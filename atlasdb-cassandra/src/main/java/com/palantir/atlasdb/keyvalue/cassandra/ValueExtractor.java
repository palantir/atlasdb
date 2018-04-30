/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Map;

import com.codahale.metrics.Meter;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Value;

class ValueExtractor extends ResultsExtractor<Value> {
    private final Map<Cell, Value> collector;
    private final Meter notLatestVisibleValueCellFilterMeter =
            getNotlatestVisibleValueCellFilterMeter(ValueExtractor.class);

    ValueExtractor(Map<Cell, Value> collector) {
        this.collector = collector;
    }

    static ValueExtractor create() {
        return new ValueExtractor(Maps.newHashMap());
    }

    @Override
    public void internalExtractResult(long startTs,
                                      ColumnSelection selection,
                                      byte[] row,
                                      byte[] col,
                                      byte[] val,
                                      long ts) {
        if (ts < startTs && selection.contains(col)) {
            Cell cell = Cell.create(row, col);
            if (!collector.containsKey(cell)) {
                collector.put(cell, Value.create(val, ts));
            } else {
                notLatestVisibleValueCellFilterMeter.mark();
            }
        } else {
            notLatestVisibleValueCellFilterMeter.mark();
        }
    }

    @Override
    public Map<Cell, Value> asMap() {
        return collector;
    }
}
