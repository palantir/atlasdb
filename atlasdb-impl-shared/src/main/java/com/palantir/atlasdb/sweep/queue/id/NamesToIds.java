/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep.queue.id;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepNameToIdTable.SweepNameToIdNamedColumn;
import com.palantir.atlasdb.schema.generated.SweepNameToIdTable.SweepNameToIdRow;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.logsafe.Preconditions;
import java.util.Collections;
import java.util.Optional;

class NamesToIds {
    private static final TargetedSweepTableFactory tableFactory = TargetedSweepTableFactory.of();
    private static final TableReference NAME_TO_ID = tableFactory.getSweepNameToIdTable(null).getTableRef();

    private final KeyValueService kvs;

    NamesToIds(KeyValueService kvs) {
        this.kvs = kvs;
    }

    SweepTableIdentifier storeAsPending(TableReference table, int nextId) {
        SweepNameToIdRow row = SweepNameToIdRow.of(table.getQualifiedName());
        Cell cell = Cell.create(row.persistToBytes(), SweepNameToIdNamedColumn.ID.getShortName());
        SweepTableIdentifier newValue = SweepTableIdentifier.pending(nextId);
        CheckAndSetRequest cas = CheckAndSetRequest.newCell(NAME_TO_ID, cell, newValue.persistToBytes());
        try {
            kvs.checkAndSet(cas);
            return newValue;
        } catch (CheckAndSetException e) {
            return currentMapping(table).get();
        }
    }

    void storeAsPending(TableReference table, int lastAttempt, int thisAttempt) {
        SweepNameToIdRow row = SweepNameToIdRow.of(table.getQualifiedName());
        Cell cell = Cell.create(row.persistToBytes(), SweepNameToIdNamedColumn.ID.getShortName());
        byte[] oldValue = SweepTableIdentifier.pending(lastAttempt).persistToBytes();
        byte[] newValue = SweepTableIdentifier.pending(thisAttempt).persistToBytes();
        CheckAndSetRequest cas = CheckAndSetRequest.singleCell(NAME_TO_ID, cell, oldValue, newValue);
        try {
            kvs.checkAndSet(cas);
        } catch (CheckAndSetException e) {
            // ignored; we're already spinning
        }
    }

    void moveToComplete(TableReference table, int id) {
        SweepNameToIdRow row = SweepNameToIdRow.of(table.getQualifiedName());
        Cell cell = Cell.create(row.persistToBytes(), SweepNameToIdNamedColumn.ID.getShortName());
        SweepTableIdentifier oldValue = SweepTableIdentifier.pending(id);
        SweepTableIdentifier newValue = SweepTableIdentifier.identified(id);
        CheckAndSetRequest request = CheckAndSetRequest.singleCell(
                NAME_TO_ID,
                cell,
                oldValue.persistToBytes(),
                newValue.persistToBytes());
        try {
            kvs.checkAndSet(request);
        } catch (CheckAndSetException e) {
            SweepTableIdentifier actual = currentMapping(table).get();
            Preconditions.checkState(newValue.equals(actual), "Unexpectedly we state changed from pending(id) to "
                    + "not(identified(id)) after identifying id as the correct value");
        }
    }

    Optional<SweepTableIdentifier> currentMapping(TableReference table) {
        SweepNameToIdRow row = SweepNameToIdRow.of(table.getQualifiedName());
        Cell cell = Cell.create(row.persistToBytes(), SweepNameToIdNamedColumn.ID.getShortName());
        return Optional.ofNullable(kvs.get(NAME_TO_ID, Collections.singletonMap(cell, Long.MAX_VALUE)).get(cell))
                .map(Value::getContents)
                .map(SweepTableIdentifier.BYTES_HYDRATOR::hydrateFromBytes);
    }
}
