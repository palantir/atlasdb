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
import io.vavr.Tuple2;
import io.vavr.collection.SortedMap;
import io.vavr.collection.TreeMap;
import java.util.Optional;

public final class DefaultTable implements Table {
    private final StructureHolder<SortedMap<Integer, Columns>> rows;

    private DefaultTable(StructureHolder<SortedMap<Integer, Columns>> rows) {
        this.rows = rows;
    }

    public static DefaultTable empty() {
        return new DefaultTable(StructureHolder.create(TreeMap::empty));
    }

    @Override
    public Optional<Integer> get(int row, int column) {
        return rows.apply(map -> map.get(row).map(columns -> columns.get(column)))
                .getOrElse(Optional.empty());
    }

    @Override
    public boolean containsKey(int row, int column) {
        return rows.apply(
                map -> map.get(row).map(columns -> columns.containsKey(column)).getOrElse(false));
    }

    @Override
    public void put(int row, int column, int value) {
        rows.with(map -> {
            Tuple2<Columns, ? extends SortedMap<Integer, Columns>> tuple =
                    map.computeIfAbsent(row, unused -> DefaultColumns.empty());
            tuple._1().put(column, value);
            return tuple._2();
        });
    }

    @Override
    public void delete(int row, int column) {
        rows.with(map -> {
            Tuple2<Columns, ? extends SortedMap<Integer, Columns>> tuple =
                    map.computeIfAbsent(row, unused -> DefaultColumns.empty());
            tuple._1().delete(column);
            return tuple._2();
        });
    }

    @Override
    public SortedMap<Integer, Optional<Integer>> getColumnsInRange(
            int row, Optional<Integer> startInclusive, Optional<Integer> endExclusive) {
        return rows.apply(map -> map.get(row))
                .map(columns -> columns.getColumnsInRange(startInclusive, endExclusive))
                .getOrElse(TreeMap::empty);
    }

    @Override
    public SortedMap<Integer, Columns> snapshot() {
        return rows.getSnapshot();
    }
}
