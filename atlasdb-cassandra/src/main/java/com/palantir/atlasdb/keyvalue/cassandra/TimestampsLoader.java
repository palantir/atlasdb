/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import java.util.Set;

import org.apache.cassandra.thrift.ConsistencyLevel;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;

class TimestampsLoader {
    private CellLoader cellLoader;

    TimestampsLoader(CellLoader cellLoader) {
        this.cellLoader = cellLoader;
    }

    Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long ts,
            ConsistencyLevel consistency) {
        CassandraKeyValueServices.AllTimestampsCollector collector =
                new CassandraKeyValueServices.AllTimestampsCollector();
        cellLoader.loadWithTs("getAllTimestamps", tableRef, cells, ts, true, collector, consistency);
        return collector.getCollectedResults();
    }
}
