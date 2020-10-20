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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TableValueStyleCache {
    private static final Logger log = LoggerFactory.getLogger(TableValueStyleCache.class);

    private final Cache<TableReference, TableValueStyle> valueStyleByTableRef =
            CacheBuilder.newBuilder().build();

    public TableValueStyle getTableType(
            final ConnectionSupplier connectionSupplier, final TableReference tableRef, TableReference metadataTable) {
        try {
            return valueStyleByTableRef.get(tableRef, () -> {
                SqlConnection conn = connectionSupplier.get();
                AgnosticResultSet results = conn.selectResultSetUnregisteredQuery(
                        String.format(
                                "SELECT table_size FROM %s WHERE table_name = ?", metadataTable.getQualifiedName()),
                        tableRef.getQualifiedName());
                Preconditions.checkArgument(
                        !results.rows().isEmpty(), "table %s not found", tableRef.getQualifiedName());

                return TableValueStyle.byId(
                        Iterables.getOnlyElement(results.rows()).getInteger("table_size"));
            });
        } catch (ExecutionException e) {
            log.error("TableValueStyle for the table {} could not be retrieved.", tableRef.getQualifiedName());
            throw Throwables.propagate(e);
        }
    }

    public void clearCacheForTable(TableReference tableRef) {
        valueStyleByTableRef.invalidate(tableRef);
    }
}
