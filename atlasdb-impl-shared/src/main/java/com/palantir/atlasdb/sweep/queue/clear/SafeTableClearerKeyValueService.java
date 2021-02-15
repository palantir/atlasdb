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

package com.palantir.atlasdb.sweep.queue.clear;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.ForwardingKeyValueService;
import com.palantir.atlasdb.sweep.queue.ImmutableTimestampSupplier;
import java.util.Set;

public class SafeTableClearerKeyValueService extends ForwardingKeyValueService {
    private final KeyValueService delegate;
    private final TableClearer tableClearer;

    public SafeTableClearerKeyValueService(ImmutableTimestampSupplier immutableTimestamp, KeyValueService delegate) {
        this.delegate = delegate;
        this.tableClearer = new DefaultTableClearer(delegate, immutableTimestamp);
    }

    @Override
    public KeyValueService delegate() {
        return delegate;
    }

    @Override
    public void dropTable(TableReference table) {
        tableClearer.dropTables(table);
    }

    @Override
    public void dropTables(Set<TableReference> tables) {
        tableClearer.dropTables(tables);
    }

    @Override
    public void truncateTable(TableReference table) {
        tableClearer.truncateTables(table);
    }

    @Override
    public void truncateTables(Set<TableReference> tables) {
        tableClearer.truncateTables(tables);
    }

    // Some people use delete(RangeRequest.all()) instead of truncate because truncates negatively affect Oracle backups
    @Override
    public void deleteRange(TableReference table, RangeRequest range) {
        if (range.equals(RangeRequest.all())) {
            tableClearer.deleteAllRowsInTables(table);
        } else {
            super.deleteRange(table, range);
        }
    }
}
