/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.rocksdb.impl;

import org.rocksdb.RocksIterator;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.util.Pair;

public class ValueRangeIterator extends RangeIterator<Value> {

    ValueRangeIterator(RocksIterator it, RangeRequest range, long maxTimestamp) {
        super(it, range, maxTimestamp);
    }

    @Override
    protected Value processCell(Pair<Cell, Long> cellAndInitialTs) {
        Cell cell = cellAndInitialTs.lhSide;
        Value ret = null;
        if (cellAndInitialTs.rhSide < maxTimestamp) {
            ret = Value.create(it.value(), cellAndInitialTs.rhSide);
        }
        for (it.next(); it.isValid(); it.next()) {
            Pair<Cell, Long> cellAndTs = RocksDbKeyValueServices.parseCellAndTs(it.key());
            if (!cellAndTs.lhSide.equals(cell)) {
                break;
            }
            if (cellAndTs.rhSide < maxTimestamp) {
                ret = Value.create(it.value(), cellAndTs.rhSide);
            }
        }
        return ret;
    }
}
