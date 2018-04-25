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

package com.palantir.atlasdb.sweep.queue;

import org.immutables.value.Value;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TableReferenceAndCell;

/**
 * Contains information about a committed write, for use by the sweep queue.
 */
@Value.Immutable
public interface WriteInfo {
    long timestamp();
    TableReferenceAndCell tableRefCell();

    static WriteInfo of(TableReferenceAndCell tableRefCell, long timestamp) {
        return ImmutableWriteInfo.builder()
                .tableRefCell(tableRefCell)
                .timestamp(timestamp)
                .build();
    }

    static WriteInfo of(TableReference tableRef, Cell cell, long timestamp) {
        return WriteInfo.of(TableReferenceAndCell.of(tableRef, cell), timestamp);
    }
}
