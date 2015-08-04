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
package com.palantir.atlasdb.keyvalue.leveldb.impl;

import java.util.Set;

import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.util.Pair;

public class HistoryRangeIterator extends RangeIterator<Set<Value>> {

    HistoryRangeIterator(DBIterator it, byte[] tablePrefix, RangeRequest range, long endTimestamp) {
        super(it, tablePrefix, range, endTimestamp);
    }

    @Override
    protected Set<Value> processCell(Cell cell) throws DBException {
        Set<Value> ret = Sets.newHashSet();
        while (it.hasNext()) {
            final Pair<Cell, Long> cellAndTs = LevelDbKeyValueUtils.parseCellAndTs(it.peekNext().getKey(), tablePrefix);
            if (!cellAndTs.lhSide.equals(cell)) {
                // this is a different cell
                break;
            }
            final long lastTs = cellAndTs.rhSide;
            if (lastTs < timestampExclusive) {
                ret.add(Value.create(it.peekNext().getValue(), lastTs));
            }
            it.next();
        }
        return ret;
    }
}
