// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.transaction.api;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowResult;

public interface ConstraintCheckingTransaction extends Transaction {
    /**
     * Returns the cell values in the key-value service, ignoring the cache of local writes.
     */
    public Map<Cell, byte[]> getIgnoringLocalWrites(String tableName, Set<Cell> cells);

    /**
     * Returns the row result values in the key-value service, ignoring the cache of local writes.
     */
    public SortedMap<byte[], RowResult<byte[]>> getRowsIgnoringLocalWrites(String tableName, Iterable<byte[]> rows);
}
