/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra.sweep;

import org.immutables.value.Value;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.Cell;

@Value.Immutable
public abstract class CellWithTimestamp {
    public abstract Cell cell();

    public abstract long timestamp();

    public static CellWithTimestamp of(Cell cell, long timestamp) {
        return ImmutableCellWithTimestamp.builder()
                .cell(cell)
                .timestamp(timestamp)
                .build();
    }

    @Value.Check
    protected final void check() {
        Preconditions.checkState(timestamp() >= -1,
                "Timestamp must be non-negative (or -1 for the sweep sentinel); got %s", timestamp());
    }
}
