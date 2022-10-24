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
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OracleDdlTable;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleter;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/**
 * Drops all Oracle Tables associated with the table and overflow table prefixes provided in the config.
 * This does _not_ clean up the AtlasDB metadata, _timestamp, and name mapping tables, as these can be shared
 * across multiple prefixes.
 */
public final class OracleNamespaceDeleter implements NamespaceDeleter {
    private final OracleNamespaceDeleterParameters parameters;

    public OracleNamespaceDeleter(OracleNamespaceDeleterParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public void deleteAllDataFromNamespace() {
        Set<TableReference> tableNamesToDrop = getAllTableNamesToDrop();
        tableNamesToDrop.stream().map(parameters.oracleDdlTableFactory()).forEach(OracleDdlTable::drop);
    }

    @Override
    public boolean isNamespaceDeletedSuccessfully() {
        return getAllTableNamesToDrop().isEmpty();
    }

    @Override
    public void close() {
        parameters.connectionSupplier().close();
    }

    private Set<TableReference> getAllTableNamesToDrop() {
        Set<String> nonOverflowTables = getAllNonOverflowTables();
        Set<String> overflowTables = getAllOverflowTables();

        OracleTableNameGetter tableNameGetter = parameters.tableNameGetter();
        ConnectionSupplier connectionSupplier = parameters.connectionSupplier();

        return Sets.union(
                tableNameGetter.getTableReferencesFromShortTableNames(connectionSupplier, nonOverflowTables),
                tableNameGetter.getTableReferencesFromShortOverflowTableNames(connectionSupplier, overflowTables));
    }

    private Set<String> getAllNonOverflowTables() {
        return getAllTablesWithPrefix(parameters.tablePrefix());
    }

    private Set<String> getAllOverflowTables() {
        return getAllTablesWithPrefix(parameters.overflowTablePrefix());
    }

    private Set<String> getAllTablesWithPrefix(String prefix) {
        return parameters
                .connectionSupplier()
                .get()
                .selectResultSetUnregisteredQuery(
                        "SELECT table_name FROM all_tables WHERE owner = upper(?) AND"
                                + " table_name LIKE upper(?) ESCAPE '\\' AND table_name NOT LIKE UPPER(?) ESCAPE '\\'",
                        withEscapedUnderscores(parameters.userId()),
                        withWildcardSuffixAndEscapedUnderscores(prefix),
                        withWildcardPrefixAndEscapedUnderscores(AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName()))
                .rows()
                .stream()
                .map(resultSetRow -> resultSetRow.getString("table_name"))
                .collect(Collectors.toSet());
    }

    private static String withWildcardSuffixAndEscapedUnderscores(String prefix) {
        return withEscapedUnderscores(prefix) + "%";
    }

    private static String withWildcardPrefixAndEscapedUnderscores(String suffix) {
        return "%" + withEscapedUnderscores(suffix);
    }

    private static String withEscapedUnderscores(String value) {
        return value.replace("_", "\\_");
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
