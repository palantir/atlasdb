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
package com.palantir.atlasdb.keyvalue.dbkvs;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

class OracleTableNameUnmapper {
    private static final SafeLogger log = SafeLoggerFactory.get(OracleTableNameUnmapper.class);

    private Cache<String, String> unmappingCache;

    OracleTableNameUnmapper() {
        unmappingCache = CacheBuilder.newBuilder().build();
    }

    @SuppressWarnings("checkstyle:NestedTryDepth")
    public String getShortTableNameFromMappingTable(
            ConnectionSupplier connectionSupplier, String tablePrefix, TableReference tableRef)
            throws TableMappingNotFoundException {
        String fullTableName = tablePrefix + DbKvs.internalTableName(tableRef);
        try {
            return unmappingCache.get(fullTableName, () -> {
                SqlConnection conn = connectionSupplier.get();
                AgnosticResultSet results = conn.selectResultSetUnregisteredQuery(
                        "SELECT short_table_name "
                                + "FROM " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE
                                + " WHERE table_name = ?",
                        fullTableName);
                if (results.size() == 0) {
                    throw new TableMappingNotFoundException("The table " + fullTableName + " does not have a mapping."
                            + "This might be because the table does not exist.");
                }

                return Iterables.getOnlyElement(results.rows()).getString("short_table_name");
            });
        } catch (ExecutionException e) {
            throw new TableMappingNotFoundException(e.getCause());
        }
    }

    public Set<String> getLongTableNamesFromMappingTable(
            ConnectionSupplier connectionSupplier, Set<String> shortTableNames) throws TableMappingNotFoundException {
        if (shortTableNames.isEmpty()) {
            return Set.of();
        }

        String placeHolders = String.join(",", Collections.nCopies(shortTableNames.size(), "?"));

        SqlConnection conn = connectionSupplier.get();
        AgnosticResultSet results = conn.selectResultSetUnregisteredQuery(
                "SELECT table_name FROM " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE + " WHERE"
                        + " LOWER(short_table_name) IN (" + placeHolders + ")",
                shortTableNames.stream().map(String::toLowerCase).toArray());
        if (results.size() != shortTableNames.size()) {
            throw new TableMappingNotFoundException("Some of the tables " + String.join(", ", shortTableNames)
                    + " do not have a mapping. This might be because these tables do not exist.");
        }
        return results.rows().stream()
                .map(result -> result.getString("table_name"))
                .collect(Collectors.toSet());
    }

    public void clearCacheForTable(String fullTableName) {
        unmappingCache.invalidate(fullTableName);
    }
}
