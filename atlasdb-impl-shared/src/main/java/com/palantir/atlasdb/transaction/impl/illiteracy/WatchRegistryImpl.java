/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.impl.illiteracy;

import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.logsafe.Preconditions;

public class WatchRegistryImpl implements WatchRegistry {
    private static final Logger log = LoggerFactory.getLogger(WatchRegistryImpl.class);

    private final Set<RowReference> rows = Sets.newConcurrentHashSet();
    private final Set<CellReference> cells = Sets.newConcurrentHashSet();
    private final ConflictDetectionManager conflictDetectionManager;

    public WatchRegistryImpl(ConflictDetectionManager conflictDetectionManager) {
        this.conflictDetectionManager = conflictDetectionManager;
    }

    @Override
    public void enableWatchForRows(Set<RowReference> rowReferences) {
        for (RowReference rowReference : rowReferences) {
            if (rows.contains(rowReference)) {
                return;
            }
            TableReference tableReference = rowReference.tableReference();
            ConflictHandler conflictHandler = conflictDetectionManager.get(tableReference);
            Preconditions.checkState(conflictHandler.lockRowsForConflicts(),
                    "For the table " + tableReference + " we don't lock rows so enabling watch for rows is kind of"
                            + " meaningless");
            rows.add(rowReference);
        }
    }

    @Override
    public Set<RowReference> disableWatchForRows(Set<RowReference> rowReferences) {
        Set<RowReference> removed = Sets.newHashSet();
        for (RowReference reference : rowReferences) {
            boolean removedIndividual = rows.remove(reference);
            if (removedIndividual) {
                removed.add(reference);
            }
        }
        return removed;
    }

    @Override
    public Set<RowReference> filterToWatchedRows(Set<RowReference> rowReferenceSet) {
        Set<RowReference> answer = rowReferenceSet.stream().filter(rows::contains).collect(Collectors.toSet());
        log.info("filter({})={}", rowReferenceSet, answer);
        return answer;
    }

    @Override
    public void enableWatchForCells(Set<CellReference> cellReferences) {
        for (CellReference cellReference : cellReferences) {
            if (cells.contains(cellReference)) {
                return;
            }
            TableReference tableReference = cellReference.tableRef();
            ConflictHandler conflictHandler = conflictDetectionManager.get(tableReference);
            Preconditions.checkState(conflictHandler.lockCellsForConflicts(),
                    "For the table " + tableReference + " we don't lock cells so enabling watch for rows is kind of"
                            + " meaningless");
            cells.add(cellReference);
        }
    }

    @Override
    public Set<CellReference> disableWatchForCells(Set<CellReference> cellReference) {
        Set<CellReference> removed = Sets.newHashSet();
        for (CellReference reference : cellReference) {
            boolean removedIndividual = cells.remove(reference);
            if (removedIndividual) {
                removed.add(reference);
            }
        }
        return removed;
    }

    @Override
    public Set<CellReference> filterToWatchedCells(Set<CellReference> cellReferenceSet) {
        return cellReferenceSet.stream().filter(cells::contains).collect(Collectors.toSet());
    }
}
