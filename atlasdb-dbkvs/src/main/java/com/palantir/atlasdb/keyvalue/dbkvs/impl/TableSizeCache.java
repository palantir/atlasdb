/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;
import com.palantir.nexus.db.sql.AgnosticResultSet;

public final class TableSizeCache {
    private static final Cache<TableReference, TableSize> tableSizeByTableRef = CacheBuilder.newBuilder().build();

    private TableSizeCache() {
        //Utility class
    }

    public static TableSize getTableSize(
            final ConnectionSupplier conns,
            final TableReference tableRef,
            TableReference metadataTable) {
        try {
            return tableSizeByTableRef.get(tableRef, () -> {
                AgnosticResultSet results = conns.get().selectResultSetUnregisteredQuery(
                        String.format(
                                "SELECT table_size FROM %s WHERE table_name = ?",
                                metadataTable.getQualifiedName()),
                        tableRef.getQualifiedName());
                Preconditions.checkArgument(
                        !results.rows().isEmpty(),
                        "table %s not found",
                        tableRef.getQualifiedName());
                return TableSize.byId(results.get(0).getInteger("table_size"));
            });
        } catch (ExecutionException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        }
    }
}
