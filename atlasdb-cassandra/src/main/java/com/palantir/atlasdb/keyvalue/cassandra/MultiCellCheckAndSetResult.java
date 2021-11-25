/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

/**
 * A version of {@link com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult} that accommodates multi-cell operations.
 */
public interface MultiCellCheckAndSetResult<T> {
    boolean successful();

    List<Cell> relevantCells();

    Map<Cell, T> existingValues();

    static <T> MultiCellCheckAndSetResult<T> success(List<Cell> cells) {
        return ImmutableSuccessfulMultiCellCheckAndSetResult.<T>builder()
                .relevantCells(cells)
                .build();
    }

    static <T> MultiCellCheckAndSetResult<T> failure(Map<Cell, T> existingValues) {
        return ImmutableUnsuccessfulMultiCellCheckAndSetResult.<T>builder()
                .addAllRelevantCells(existingValues.keySet())
                .existingValues(existingValues)
                .build();
    }

    @Value.Immutable
    interface SuccessfulMultiCellCheckAndSetResult<T> extends MultiCellCheckAndSetResult<T> {
        @Override
        default boolean successful() {
            return true;
        }

        @Override
        default Map<Cell, T> existingValues() {
            throw new SafeIllegalStateException("Cannot query existing values from a successful result!");
        }
    }

    @Value.Immutable
    interface UnsuccessfulMultiCellCheckAndSetResult<T> extends MultiCellCheckAndSetResult<T> {
        @Override
        default boolean successful() {
            return false;
        }
    }
}
