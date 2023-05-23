/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.invariant;

import com.palantir.atlasdb.keyvalue.api.cache.StructureHolder;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import java.util.Optional;
import java.util.TreeMap;

public final class VersionedTableView<K, V> {
    private final java.util.NavigableMap<Long, Map<K, V>> tableViews = new TreeMap<>();

    public void put(Long commitTimestamp, Map<K, V> tableView) {
        tableViews.put(commitTimestamp, tableView);
    }

    public StructureHolder<Map<K, V>> getView(Long startTimestamp) {
        return StructureHolder.create(() -> Optional.ofNullable(tableViews.lowerEntry(startTimestamp))
                .map(java.util.Map.Entry::getValue)
                .orElseGet(HashMap::empty));
    }

    public StructureHolder<Map<K, V>> getLatestTableView() {
        return StructureHolder.create(() -> Optional.ofNullable(tableViews.lastEntry())
                .map(java.util.Map.Entry::getValue)
                .orElseGet(HashMap::empty));
    }
}
