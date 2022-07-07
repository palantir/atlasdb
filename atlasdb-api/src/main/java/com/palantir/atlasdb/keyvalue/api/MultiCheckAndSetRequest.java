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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
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
    TableReference tableRef();

    byte[] rowName();

    Map<Cell, byte[]> expected();

    Map<Cell, byte[]> updates();

    @Value.Check
    default void check() {
        Set<ByteBuffer> rowsForExpectedCells = getRowsForCells(expected());
        Preconditions.checkState(
                rowsForExpectedCells.isEmpty() || hasConsistentRowName(rowsForExpectedCells),
                "Only expects values for cells in the same row.");

        Preconditions.checkState(hasConsistentRowName(getRowsForCells(updates())), "Can only update cells in one row.");
    }

    private boolean hasConsistentRowName(Set<ByteBuffer> rowsForExpectedCells) {
        if (rowsForExpectedCells.size() == 1) {
            return Objects.equals(Iterables.getOnlyElement(rowsForExpectedCells), ByteBuffer.wrap(rowName()));
        }
        return false;
    }

    private Set<ByteBuffer> getRowsForCells(Map<Cell, byte[]> cellMap) {
        return cellMap.keySet().stream()
                .map(Cell::getRowName)
                .map(ByteBuffer::wrap)
                .collect(Collectors.toSet());
    }

    static MultiCheckAndSetRequest newCells(TableReference table, byte[] rowName, Map<Cell, byte[]> updates) {
        return builder().tableRef(table).rowName(rowName).updates(updates).build();
    }

    static MultiCheckAndSetRequest multipleCells(
            TableReference table, byte[] rowName, Map<Cell, byte[]> expected, Map<Cell, byte[]> updates) {
        return builder()
                .tableRef(table)
                .rowName(rowName)
                .expected(expected)
                .updates(updates)
                .build();
    }

    static ImmutableMultiCheckAndSetRequest.Builder builder() {
        return ImmutableMultiCheckAndSetRequest.builder();
    }
}
