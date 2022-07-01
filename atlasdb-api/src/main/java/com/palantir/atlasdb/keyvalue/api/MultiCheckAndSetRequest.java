/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import com.google.common.collect.Iterables;
import com.palantir.logsafe.Preconditions;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/**
 * A request to be supplied to KeyValueService.mulitCheckAndSet.
 *
 * {@link #tableRef()} the {@link TableReference} on which updates are to be performed.
 * {@link #rowName()} the {@link Cell} row to update.
 * {@link #expected()} expected current values of cells.
 * {@link #updates()} the desired values for cells.
 */
@Value.Immutable
public interface MultiCheckAndSetRequest {
    @Value.Parameter
    TableReference tableRef();

    @Value.Parameter
    byte[] rowName();

    @Value.Parameter
    Map<Cell, byte[]> expected();

    @Value.Parameter
    Map<Cell, byte[]> updates();

    @Value.Check
    default void check() {
        Set<byte[]> rowsForExpectedCells = getRowsForCells(expected());
        Preconditions.checkState(
                rowsForExpectedCells.size() == 1
                        && Arrays.equals(Iterables.getOnlyElement(rowsForExpectedCells), rowName()),
                "Only expect values for cells in the same row.");

        Set<byte[]> rowsForUpdates = getRowsForCells(updates());
        Preconditions.checkState(
                rowsForUpdates.size() == 1 && Arrays.equals(Iterables.getOnlyElement(rowsForUpdates), rowName()),
                "Can only update cells in one row.");
    }

    private Set<byte[]> getRowsForCells(Map<Cell, byte[]> cellMap) {
        return expected().keySet().stream().map(Cell::getRowName).collect(Collectors.toSet());
    }

    static ImmutableMultiCheckAndSetRequest.Builder builder() {
        return ImmutableMultiCheckAndSetRequest.builder();
    }
}
