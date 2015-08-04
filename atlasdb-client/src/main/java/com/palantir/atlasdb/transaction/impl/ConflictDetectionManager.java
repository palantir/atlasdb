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
package com.palantir.atlasdb.transaction.impl;

import java.util.Map;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class ConflictDetectionManager {
    private final RecomputingSupplier<Map<String, ConflictHandler>> supplier;
    private final Map<String, ConflictHandler> overrides;

    public ConflictDetectionManager(RecomputingSupplier<Map<String, ConflictHandler>> supplier) {
        this.supplier = supplier;
        this.overrides = Maps.newConcurrentMap();
    }

    public void setConflictDetectionMode(String table, ConflictHandler handler) {
        overrides.put(table, handler);
    }

    public void removeConflictDetectionMode(String table) {
        overrides.remove(table);
    }

    public boolean isEmptyOrContainsTable(String tableName) {
        if (supplier.get().isEmpty() && overrides.isEmpty()) {
            return true;
        }
        if (supplier.get().containsKey(tableName) || overrides.containsKey(tableName)) {
            return true;
        }
        return false;
    }

    public Map<String, ConflictHandler> get() {
        Map<String, ConflictHandler> ret = Maps.newHashMap(supplier.get());
        ret.putAll(overrides);
        return ret;
    }

    public void recompute() {
        supplier.recompute();
    }
}
