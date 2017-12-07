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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class SweepDeleter {

    private final TableReference tableRef;
    private final KeyValueService keyValueService;

    public SweepDeleter(TableReference tableRef, KeyValueService keyValueService) {
        this.tableRef = tableRef;
        this.keyValueService = keyValueService;
    }

    public void delete(List<Write> writes) {
        Map<Cell, Long> lastTimestampByCell = new HashMap<>();

        for (Write write : writes) {
            lastTimestampByCell.put(Cell.create(write.rowName(), write.columnName()), write.timestamp());
        }

        keyValueService.deleteAllTimestamps(tableRef, lastTimestampByCell);
    }

}
