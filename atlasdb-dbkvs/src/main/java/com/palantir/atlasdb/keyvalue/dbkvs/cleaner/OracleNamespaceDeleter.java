/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs.cleaner;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleDdlTable;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleter;
import com.palantir.common.base.Throwables;
import com.palantir.common.exception.TableMappingNotFoundException;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/**
 * Drops all Oracle Tables associated with the table and overflow table prefixes provided in the config.
 * This does _not_ clean up the AtlasDB metadata and name mapping tables, as these can be shared across multiple
 * prefixes.
 */
public class OracleNamespaceDeleter implements NamespaceDeleter {
    private final ConnectionSupplier connectionSupplier;
    private final String wildcardEscapedTablePrefix;
    private final String wildcardEscapedOverflowTablePrefix;
    private final String escapedUserId;

    private final Function<TableReference, OracleDdlTable> oracleDdlTableFactory;
    private final OracleTableNameGetter tableNameGetter;

    public OracleNamespaceDeleter(OracleNamespaceDeleterParameters parameters) {
        this.wildcardEscapedTablePrefix = withWildcardSuffix(withEscapedUnderscores(parameters.tablePrefix()));
        this.wildcardEscapedOverflowTablePrefix =
                withWildcardSuffix(withEscapedUnderscores(parameters.overflowTablePrefix()));
        this.escapedUserId = withEscapedUnderscores(parameters.userId());
        this.connectionSupplier = parameters.connectionSupplier();
        this.oracleDdlTableFactory = parameters.oracleDdlTableFactory();
        this.tableNameGetter = parameters.tableNameGetter();
    }

    @Override
    public void deleteAllDataFromNamespace() {
        Set<TableReference> tableNamesToDrop = getAllTableNamesToDrop();
        tableNamesToDrop.stream().map(oracleDdlTableFactory).forEach(OracleDdlTable::drop);
    }

    @Override
    public boolean isNamespaceDeletedSuccessfully() {
        return getNumberOfTables() == 0;
    }

    @Override
    public void close() {
        connectionSupplier.close();
    }

    private Set<TableReference> getAllTableNamesToDrop() {
        Set<String> nonOverflowTables = getAllNonOverflowTables();
        Set<String> overflowTables = getAllOverflowTables();

        try {
            return Sets.union(
                    tableNameGetter.getTableReferencesFromShortTableNames(connectionSupplier, nonOverflowTables),
                    tableNameGetter.getTableReferencesFromShortOverflowTableNames(connectionSupplier, overflowTables));
        } catch (TableMappingNotFoundException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private Set<String> getAllNonOverflowTables() {
        return getAllTablesWithPrefix(wildcardEscapedTablePrefix);
    }

    private Set<String> getAllOverflowTables() {
        return getAllTablesWithPrefix(wildcardEscapedOverflowTablePrefix);
    }

    private Set<String> getAllTablesWithPrefix(String prefix) {
        return connectionSupplier
                .get()
                .selectResultSetUnregisteredQuery(
                        "SELECT table_name FROM all_tables WHERE owner = upper(?) AND"
                                + " table_name LIKE upper(?) ESCAPE '\\'",
                        escapedUserId,
                        prefix)
                .rows()
                .stream()
                .map(resultSetRow -> resultSetRow.getString("table_name"))
                .collect(Collectors.toSet());
    }

    private long getNumberOfTables() {
        return connectionSupplier
                .get()
                .selectCount(
                        "all_tables",
                        "owner = upper(?) AND (table_name LIKE upper(?) ESCAPE '\\' OR table_name"
                                + " LIKE upper(?) ESCAPE '\\')",
                        escapedUserId,
                        wildcardEscapedTablePrefix,
                        wildcardEscapedOverflowTablePrefix);
    }

    private static String withWildcardSuffix(String tableName) {
        return tableName + "%";
    }

    private static String withEscapedUnderscores(String prefix) {
        return prefix.replace("_", "\\_");
    }

    @Value.Immutable
    public interface OracleNamespaceDeleterParameters {
        String tablePrefix();

        String overflowTablePrefix();

        String userId();

        ConnectionSupplier connectionSupplier();

        OracleTableNameGetter tableNameGetter();

        Function<TableReference, OracleDdlTable> oracleDdlTableFactory();

        static ImmutableOracleNamespaceDeleterParameters.Builder builder() {
            return ImmutableOracleNamespaceDeleterParameters.builder();
        }
    }
}
