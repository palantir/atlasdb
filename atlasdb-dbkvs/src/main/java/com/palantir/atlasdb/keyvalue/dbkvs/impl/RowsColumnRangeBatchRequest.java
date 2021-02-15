/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.logsafe.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * Represents a batch to load in {@link com.palantir.atlasdb.keyvalue.api.KeyValueService#getRowsColumnRange(
 * com.palantir.atlasdb.keyvalue.api.TableReference, Iterable, ColumnRangeSelection, int, long)}. The overall iteration
 * order returns all requested columns for the first row, followed by all requested columns for the second row, and so
 * forth. Hence, a single batch consists of some contiguous group of rows to fully load, plus optionally a first row
 * that has a different starting column and optionally a last row where we load a number of columns less than the total.
 */
@Value.Immutable
public abstract class RowsColumnRangeBatchRequest {
    public abstract Optional<Map.Entry<byte[], BatchColumnRangeSelection>> getPartialFirstRow();

    public abstract List<byte[]> getRowsToLoadFully();
    /**
     * Can only be null if {@link #getRowsToLoadFully()} is empty.
     */
    @Nullable
    public abstract ColumnRangeSelection getColumnRangeSelection();

    public abstract Optional<Map.Entry<byte[], BatchColumnRangeSelection>> getPartialLastRow();

    @Value.Check
    protected void check() {
        Preconditions.checkState(
                getColumnRangeSelection() != null || getRowsToLoadFully().isEmpty(),
                "Must specify a column range selection when loading full rows.");
    }
}
