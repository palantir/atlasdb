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
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import io.vavr.collection.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

public final class TableView {
    private final NavigableMap<Long, Map<TableAndWorkloadCell, ValueAndTimestamp>> tableViews = new TreeMap<>();

    public void put(Long commitTimestamp, Map<TableAndWorkloadCell, ValueAndTimestamp> tableView) {
        tableViews.put(commitTimestamp, tableView);
    }

    public StructureHolder<io.vavr.collection.Map<TableAndWorkloadCell, ValueAndTimestamp>> getView(
            Long startTimestamp) {
        return StructureHolder.create(
                () -> Optional.ofNullable(tableViews.lowerEntry(startTimestamp).getValue())
                        .orElseGet(io.vavr.collection.HashMap::empty));
    }

    public StructureHolder<io.vavr.collection.Map<TableAndWorkloadCell, ValueAndTimestamp>> getLatestTableView() {
        return StructureHolder.create(() ->
                Optional.ofNullable(tableViews.lastEntry().getValue()).orElseGet(io.vavr.collection.HashMap::empty));
    }
}
