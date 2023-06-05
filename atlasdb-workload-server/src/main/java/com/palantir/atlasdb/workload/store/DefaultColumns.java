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

package com.palantir.atlasdb.workload.store;

import com.palantir.atlasdb.keyvalue.api.cache.StructureHolder;
import io.vavr.collection.SortedMap;
import io.vavr.collection.TreeMap;
import java.util.Optional;

/**
 * Models columns for a given workload row in a table.
 * <p>
 * Note that deletes are explicitly tracked as empty values, for the purposes of certain invariants that want to check
 * that they actually happened.
 */
public final class DefaultColumns implements Columns {
    private final StructureHolder<SortedMap<Integer, Optional<Integer>>> structureHolder;

    private DefaultColumns(StructureHolder<SortedMap<Integer, Optional<Integer>>> structureHolder) {
        this.structureHolder = structureHolder;
    }

    public static DefaultColumns empty() {
        return new DefaultColumns(StructureHolder.create(TreeMap::empty));
    }

    @Override
    public Optional<Integer> get(int column) {
        return structureHolder.apply(map -> map.get(column)).getOrElse(Optional.empty());
    }

    @Override
    public boolean containsKey(int column) {
        return structureHolder.apply(map -> map.containsKey(column));
    }

    @Override
    public void put(int column, int value) {
        structureHolder.with(map -> map.put(column, Optional.of(value)));
    }

    @Override
    public void delete(int column) {
        structureHolder.with(map -> map.put(column, Optional.empty()));
    }

    @Override
    public SortedMap<Integer, Optional<Integer>> getColumnsInRange(
            Optional<Integer> startInclusive, Optional<Integer> endExclusive) {
        // TODO (jkong): To do this more efficiently, this needs to be implemented in the Vavr layer. However, an
        //  algorithm that is O(N) instead of O(log N) in the columns probably isn't the worst for now, given we
        //  don't expect to have many columns in a single row.
        return structureHolder.apply(map -> map.dropWhile(
                        tuple -> startInclusive.map(start -> tuple._1() < start).orElse(false))
                .takeWhile(tuple -> endExclusive.map(end -> end > tuple._1()).orElse(true))
                .filter(tuple -> tuple._2().isPresent()));
    }

    @Override
    public SortedMap<Integer, Optional<Integer>> snapshot() {
        return structureHolder.getSnapshot();
    }
}
