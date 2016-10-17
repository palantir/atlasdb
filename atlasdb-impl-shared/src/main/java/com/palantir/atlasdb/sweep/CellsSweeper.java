/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.sweep;

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public class CellsSweeper {
    private final TransactionManager txManager;
    private final KeyValueService keyValueService;
    private final Collection<Follower> followers;

    public CellsSweeper(
            TransactionManager txManager,
            KeyValueService keyValueService,
            Collection<Follower> followers) {
        this.txManager = txManager;
        this.keyValueService = keyValueService;
        this.followers = followers;
    }

    public void sweepCells(
            TableReference tableRef,
            Multimap<Cell, Long> cellTsPairsToSweep,
            Set<Cell> sentinelsToAdd) {
        if (cellTsPairsToSweep.isEmpty()) {
            return;
        }

        for (Follower follower : followers) {
            follower.run(txManager, tableRef, cellTsPairsToSweep.keySet(), Transaction.TransactionType.HARD_DELETE);
        }
        if (!sentinelsToAdd.isEmpty()) {
            keyValueService.addGarbageCollectionSentinelValues(
                    tableRef,
                    sentinelsToAdd);
        }
        keyValueService.delete(tableRef, cellTsPairsToSweep);
    }
}
