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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
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

    public Map<String, String> getShortToLongTableNamesFromMappingTable(
            ConnectionSupplier connectionSupplier, Set<String> shortTableNames) {
        if (shortTableNames.isEmpty()) {
            return Map.of();
        }
        ImmutableMap.Builder<String, String> shortToLongTableNames = ImmutableMap.builder();
        for (List<String> batch :
                Iterables.partition(shortTableNames, AtlasDbConstants.MINIMUM_IN_CLAUSE_EXPRESSION_LIMIT)) {
            shortToLongTableNames.putAll(getBatchOfLongTableNamesFromMappingTable(connectionSupplier, batch));
        }
        return shortToLongTableNames.buildOrThrow();
    }

    private Map<String, String> getBatchOfLongTableNamesFromMappingTable(
            ConnectionSupplier connectionSupplier, Collection<String> shortTableNames) {
        String placeHolders = String.join(",", Collections.nCopies(shortTableNames.size(), "LOWER(?)"));

        SqlConnection conn = connectionSupplier.get();

        // Our Oracle name mapping table contains entries in the user-provided case. However, we don't necessarily
        // have the user-supplied casing when calling this method (e.g, the short table names were retrieved from
        // all_tables, which are stored in caps unless explicitly opted out of on a per-entry basis).
        // Thus, we need to do a case-insensitive search, implemented by LOWER(x) = LOWER(y). For completeness sake, one
        // likely also needs to do OR UPPER(x) = UPPER(y), but there's little benefit in doing it here.

        // We use the Oracle LOWER function rather than mapping to lower case on the client to ensure that we're
        // using the same locale and conversion.
        Map<String, String> shortTableToLongTableMapping = runIgnoringTableNotFound(
                () -> {
                    AgnosticResultSet results = conn.selectResultSetUnregisteredQuery(
                            "SELECT short_table_name, table_name FROM " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE
                                    + " WHERE" + " LOWER(short_table_name) IN (" + placeHolders + ")",
                            shortTableNames.toArray());

                    return results.rows().stream()
                            .collect(Collectors.toMap(
                                    row -> row.getString("short_table_name"), row -> row.getString("table_name")));
                },
                Map::of);

        verifyMappingSetSizeNotLargerThanExpected(shortTableToLongTableMapping, shortTableNames);

        return shortTableToLongTableMapping;
    }

    private void verifyMappingSetSizeNotLargerThanExpected(
            Map<String, String> shortTableToLongTableMapping, Collection<String> expectedShortTableNames) {
        Preconditions.checkState(
                shortTableToLongTableMapping.size() <= expectedShortTableNames.size(),
                "There are more returned long table names than provided short table names. This likely indicates"
                        + " a bug in AtlasDB",
                SafeArg.of("numLongTables", shortTableToLongTableMapping.size()),
                SafeArg.of("numShortTables", expectedShortTableNames.size()),
                UnsafeArg.of("returnedMapping", shortTableToLongTableMapping),
                UnsafeArg.of("expectedShortTableNames", expectedShortTableNames));
    }

    public void clearCacheForTable(String fullTableName) {
        unmappingCache.invalidate(fullTableName);
    }

    private static <T> T runIgnoringTableNotFound(Supplier<T> task, Supplier<T> defaultValueSupplier) {
        try {
            return task.get();
        } catch (PalantirSqlException e) {
            if (!e.getMessage().contains(OracleErrorConstants.ORACLE_NOT_EXISTS_ERROR)) {
                log.error("Error occurred trying to execute the task", e);
                throw e;
            }
            return defaultValueSupplier.get();
        }
    }
}
