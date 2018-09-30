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

package com.palantir.atlasdb.sweep.queue.id;

import java.util.NoSuchElementException;
import java.util.Optional;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;

/**
 * Creates a dictionary of table references to shorter (integral) identifiers.
 * <p>
 * Algorithm is slightly more complicated than usual Atlas code because it cannot assume transactions. It works as
 * follows.
 * <p>
 * 1. If table exists in NamesToIds and is 'identified', we are done.
 * 2. If table is 'pending', take its id. Otherwise PutUnlessExists in the current highest known id + 1 in IdsToNames
 *    into the NamesToIds table. In case of failure, take the value that now must exist (and if now 'identified', we
 *    are done).
 * 3. PutUnlessExists that value into IdsToNames. If success or it already existed with the same value, go to 4,
 *    otherwise 5.
 * 4. CAS NamesToIds from 'pending' to 'identified'.
 * 5. CAS NamesToIds from its current value to the current highest known id + 1; the current best candidate has been
 *    already used by a concurrent writer.
 * <p>
 */
public final class SweepTableIndices {
    private final IdsToNames idToNames;
    private final NamesToIds namesToIds;
    private final LoadingCache<TableReference, Integer> tableIndices;

    SweepTableIndices(IdsToNames idsToNames, NamesToIds namesToIds) {
        this.idToNames = idsToNames;
        this.namesToIds = namesToIds;
        this.tableIndices = Caffeine.newBuilder().maximumSize(20_000).build(this::loadUncached);
    }

    public SweepTableIndices(KeyValueService kvs) {
        this(new IdsToNames(kvs), new NamesToIds(kvs));
    }

    public int getTableId(TableReference table) {
        return tableIndices.get(table);
    }

    public TableReference getTableReference(int tableId) {
        return idToNames.get(tableId)
                .orElseThrow(() -> new NoSuchElementException("Id " + tableId + " does not exist"));
    }

    private int loadUncached(TableReference table) {
        while (true) {
            Optional<SweepTableIdentifier> identifier = namesToIds.currentMapping(table);
            if (identifier.isPresent() && !identifier.get().isPending()) {
                return identifier.get().identifier();
            }
            SweepTableIdentifier afterPendingPut = namesToIds.storeAsPending(table, idToNames.getNextId());
            if (!afterPendingPut.isPending()) {
                return afterPendingPut.identifier();
            }
            boolean assigmentWasSuccessful = idToNames.storeNewMapping(table, afterPendingPut.identifier());
            if (assigmentWasSuccessful) {
                namesToIds.moveToComplete(table, afterPendingPut.identifier());
                return afterPendingPut.identifier();
            }
            namesToIds.storeAsPending(table, afterPendingPut.identifier(), idToNames.getNextId());
        }
    }
}
