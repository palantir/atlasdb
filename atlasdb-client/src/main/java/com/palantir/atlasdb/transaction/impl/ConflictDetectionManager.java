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
package com.palantir.atlasdb.transaction.impl;

import java.util.Map;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class ConflictDetectionManager {
    private final RecomputingSupplier<Map<TableReference, ConflictHandler>> supplier;
    private final Map<TableReference, ConflictHandler> overrides;

    public ConflictDetectionManager(RecomputingSupplier<Map<TableReference, ConflictHandler>> supplier) {
        this.supplier = supplier;
        this.overrides = Maps.newConcurrentMap();
    }

    public void setConflictDetectionMode(TableReference table, ConflictHandler handler) {
        overrides.put(table, handler);
    }

    public void removeConflictDetectionMode(TableReference table) {
        overrides.remove(table);
    }

    public boolean isEmptyOrContainsTable(TableReference tableRef) {
        Map<TableReference, ConflictHandler> tableToConflictHandler = supplier.get();
        if (tableToConflictHandler.isEmpty() && overrides.isEmpty()) {
            return true;
        }
        if (tableToConflictHandler.containsKey(tableRef) || overrides.containsKey(tableRef)) {
            return true;
        }
        return false;
    }

    public Map<TableReference, ConflictHandler> get() {
        Map<TableReference, ConflictHandler> ret = Maps.newHashMap(supplier.get());
        ret.putAll(overrides);
        return ret;
    }

    public void recompute() {
        supplier.recompute();
    }
}
