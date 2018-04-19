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

package com.palantir.atlasdb.sweep.queue;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.Sweeper;

public class SweepDeleterImpl implements SweepDeleter {

    private final TableReference table;
    private final KeyValueService kvs;
    private final Sweeper sweeper;

    public SweepDeleterImpl(TableReference table, KeyValueService kvs, Sweeper sweeper) {
        this.table = table;
        this.kvs = kvs;
        this.sweeper = sweeper;
    }

    @Override
    public void sweep(Collection<WriteInfo> writes) {
        Map<Cell, Long> maxTimestampByCell = getMaxDeletionTimestamps(writes);

        if (sweeper.shouldAddSentinels()) {
            kvs.addGarbageCollectionSentinelValues(table, maxTimestampByCell.keySet());
        }

        kvs.deleteAllTimestamps(table, maxTimestampByCell);
    }

    private Map<Cell, Long> getMaxDeletionTimestamps(Collection<WriteInfo> writes) {
        Map<Cell, Long> deleteTimestampByCell = Maps.newHashMap();

        for (WriteInfo write : writes) {
            // todo(gmaretic): since we don't know about deletes, makes it awkward for thorough sweep?
            deleteTimestampByCell.merge(write.tableRefCell().cell(), write.timestamp(), Long::max);
        }

        return deleteTimestampByCell;
    }
}
