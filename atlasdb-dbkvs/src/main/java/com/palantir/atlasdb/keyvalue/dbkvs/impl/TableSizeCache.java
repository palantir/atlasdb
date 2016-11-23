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

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;

public final class TableSizeCache {
    private static final Logger log = LoggerFactory.getLogger(TableSizeCache.class);

    private final ConnectionSupplier conns;
    private final TableReference metadataTable;

    public TableSizeCache(ConnectionSupplier conns, TableReference metadataTable) {
        this.conns = conns;
        this.metadataTable = metadataTable;
    }

    private LoadingCache<TableReference, TableSize> tableSizeByTableRef = CacheBuilder.newBuilder().build(
            new CacheLoader<TableReference, TableSize>() {
                @Override
                public TableSize load(TableReference tableRef) throws Exception {
                    SqlConnection conn = null;
                    try {
                        conn = conns.getNewUnsharedConnection();
                        AgnosticResultSet results = conn.selectResultSetUnregisteredQuery(
                                String.format(
                                        "SELECT table_size FROM %s WHERE table_name = ?",
                                        metadataTable.getQualifiedName()),
                                tableRef.getQualifiedName());
                        Preconditions.checkArgument(
                                !results.rows().isEmpty(),
                                "table %s not found",
                                tableRef.getQualifiedName());

                        return TableSize.byId(Iterables.getOnlyElement(results.rows()).getInteger("table_size"));
                    } finally {
                        if (conn != null) {
                            try {
                                conn.getUnderlyingConnection().close();
                            } catch (SQLException e) {
                                log.error("Couldn't cleanup SQL connection while retrieving table size.", e);
                            }
                        }
                    }
                }
            }
    );

    public TableSize getTableSize(TableReference tableRef) {
        try {
            return tableSizeByTableRef.get(tableRef);
        } catch (ExecutionException e) {
            log.error("TableSize for the table {} could not be retrieved.", tableRef.getQualifiedName());
            throw Throwables.propagate(e);
        }
    }

    public void clearCacheForTable(TableReference tableRef) {
        tableSizeByTableRef.invalidate(tableRef);
    }
}
