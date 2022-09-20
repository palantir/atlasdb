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
import com.palantir.atlasdb.NamespaceCleaner;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleDdlTable;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public class OracleNamespaceCleaner implements NamespaceCleaner {
    private static final String LIST_ALL_TABLES =
            "SELECT table_name FROM all_tables WHERE owner = upper(?) AND table_name LIKE upper(?) ESCAPE '\\'";
    private final ConnectionSupplier connectionSupplier;
    private final String escapedTablePrefix;
    private final String escapedOverflowTablePrefix;
    private final String escapedUserId;

    private final Function<TableReference, OracleDdlTable> oracleDdlTableFactory;
    private final OracleTableNameGetter tableNameGetter;

    public OracleNamespaceCleaner(OracleNamespaceCleanerParameters parameters) {
        this.escapedTablePrefix = escapeUnderscores(parameters.tablePrefix());
        this.escapedOverflowTablePrefix = escapeUnderscores(parameters.overflowTablePrefix());
        this.escapedUserId = escapeUnderscores(parameters.userId());
        this.connectionSupplier = parameters.connectionSupplier();
        this.oracleDdlTableFactory = parameters.oracleDdlTableFactory();
        this.tableNameGetter = parameters.tableNameGetter();
    }

    @Override
    public void dropAllTables() {
        Set<TableReference> tableNamesToDrop = getAllTableNamesToDrop();
        tableNamesToDrop.stream().map(oracleDdlTableFactory).forEach(OracleDdlTable::drop);
        // There is no IF EXISTS. DDL commands perform an implicit commit. If we fail, we should just retry by
        // dropping the namespace again!
    }

    @Override
    public boolean areAllTablesSuccessfullyDropped() {
        return getAllNonOverflowTables().size() == 0 && getAllOverflowTables().size() == 0;
    }

    @Override
    public void close() {
        connectionSupplier.close();
    }

    private Set<TableReference> getAllTableNamesToDrop() {
        Set<String> nonOverflowTables = getTableNamesFromResultSet(getAllNonOverflowTables());
        Set<String> overflowTables = getTableNamesFromResultSet(getAllOverflowTables());

        try {
            return Sets.union(
                    tableNameGetter.getTableReferencesFromShortTableNames(connectionSupplier, nonOverflowTables),
                    tableNameGetter.getTableReferencesFromShortOverflowTableNames(connectionSupplier, overflowTables));
        } catch (TableMappingNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private Set<String> getTableNamesFromResultSet(AgnosticResultSet resultSet) {
        return resultSet.rows().stream()
                .map(resultSetRow -> resultSetRow.getString("table_name"))
                .collect(Collectors.toSet());
    }

    private AgnosticResultSet getAllNonOverflowTables() {
        return getAllTables(escapedTablePrefix);
    }

    private AgnosticResultSet getAllOverflowTables() {
        return getAllTables(escapedOverflowTablePrefix);
    }

    private AgnosticResultSet getAllTables(String prefix) {
        return connectionSupplier
                .get()
                .selectResultSetUnregisteredQuery(LIST_ALL_TABLES, escapedUserId, withWildcardSuffix(prefix));
    }

    private static String withWildcardSuffix(String tableName) {
        return tableName + "%";
    }

    private static String escapeUnderscores(String prefix) {
        return prefix.replace("_", "\\_");
    }

    @Value.Immutable
    public interface OracleNamespaceCleanerParameters {
        String tablePrefix();

        String overflowTablePrefix();

        String userId();

        ConnectionSupplier connectionSupplier();

        OracleTableNameGetter tableNameGetter();

        Function<TableReference, OracleDdlTable> oracleDdlTableFactory();

        static ImmutableOracleNamespaceCleanerParameters.Builder builder() {
            return ImmutableOracleNamespaceCleanerParameters.builder();
        }
    }
}
