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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class ConflictDetectionManager {
    private final Supplier<Map<TableReference, ConflictHandler>> supplier;
    private final Map<TableReference, ConflictHandler> overrides;
    private final RecomputingSupplier<Map<TableReference, ConflictHandler>> cachedResult;

    /**
     * @param supplier A {@link Supplier} which returns the current conflict detection
     *                 configuration to the database. Note that the result is cached,
     *                 so changes to configuration must be followed by a call to
     *                 {@link #recompute()} in order to be picked up.
     */
    ConflictDetectionManager(Supplier<Map<TableReference, ConflictHandler>> supplier) {
        this.supplier = supplier;
        this.overrides = Maps.newConcurrentMap();
        this.cachedResult = RecomputingSupplier.create(() -> {
            ImmutableMap.Builder<TableReference, ConflictHandler> ret = ImmutableMap.builder();
            ret.putAll(this.supplier.get());
            ret.putAll(overrides);
            return ret.build();
        });
    }

    public void setConflictDetectionMode(TableReference table, ConflictHandler handler) {
        overrides.put(table, handler);
        cachedResult.recompute();
    }

    public void removeConflictDetectionMode(TableReference table) {
        overrides.remove(table);
        cachedResult.recompute();
    }

    public boolean isEmptyOrContainsTable(TableReference tableRef) {
        Map<TableReference, ConflictHandler> tableToConflict = cachedResult.get();
        if (tableToConflict.isEmpty() || tableToConflict.containsKey(tableRef)) {
            return true;
        }
        return false;
    }

    public Map<TableReference, ConflictHandler> get() {
        return cachedResult.get();
    }

    public void recompute() {
        cachedResult.recompute();
    }
}
