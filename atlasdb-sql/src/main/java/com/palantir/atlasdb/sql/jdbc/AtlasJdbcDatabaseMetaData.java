package com.palantir.atlasdb.sql.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

public class AtlasJdbcDatabaseMetaData implements DatabaseMetaData {
    private final AtlasJdbcConnection conn;

    public AtlasJdbcDatabaseMetaData(AtlasJdbcConnection connection) {
        this.conn = connection;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        throw new SQLException("Unsupported method allProceduresAreCallable");
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        throw new SQLException("Unsupported method allTablesAreSelectable");
    }

    @Override
    public String getURL() throws SQLException {
        return conn.getURL();
    }

    @Override
    public String getUserName() throws SQLException {
        return conn.getUserName();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw new SQLException("Unsupported method isReadOnly");
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        throw new SQLException("Unsupported method nullsAreSortedHigh");
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        throw new SQLException("Unsupported method nullsAreSortedLow");
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        throw new SQLException("Unsupported method nullsAreSortedAtStart");
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        throw new SQLException("Unsupported method nullsAreSortedAtEnd");
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "AtlasDB";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "0.0";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "AtlasDB SQL Driver";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return getDriverMajorVersion() + "." + getDriverMinorVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return AtlasJdbcDriver.MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return AtlasJdbcDriver.MINOR_VERSION;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        throw new SQLException("Unsupported method usesLocalFiles");
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        throw new SQLException("Unsupported method usesLocalFilePerTable");
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        throw new SQLException("Unsupported method supportsMixedCaseIdentifiers");
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        throw new SQLException("Unsupported method storesUpperCaseIdentifiers");
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        throw new SQLException("Unsupported method storesLowerCaseIdentifiers");
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        throw new SQLException("Unsupported method storesMixedCaseIdentifiers");
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Unsupported method supportsMixedCaseQuotedIdentifiers");
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Unsupported method storesUpperCaseQuotedIdentifiers");
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Unsupported method storesLowerCaseQuotedIdentifiers");
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        throw new SQLException("Unsupported method storesMixedCaseQuotedIdentifiers");
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        throw new SQLException("Unsupported method getIdentifierQuoteString");
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        throw new SQLException("Unsupported method getSQLKeywords");
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        throw new SQLException("Unsupported method getNumericFunctions");
    }

    @Override
    public String getStringFunctions() throws SQLException {
        throw new SQLException("Unsupported method getStringFunctions");
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        throw new SQLException("Unsupported method getSystemFunctions");
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        throw new SQLException("Unsupported method getTimeDateFunctions");
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        throw new SQLException("Unsupported method getSearchStringEscape");
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        throw new SQLException("Unsupported method getExtraNameCharacters");
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        throw new SQLException("Unsupported method supportsAlterTableWithAddColumn");
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        throw new SQLException("Unsupported method supportsAlterTableWithDropColumn");
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        throw new SQLException("Unsupported method supportsColumnAliasing");
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        throw new SQLException("Unsupported method nullPlusNonNullIsNull");
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        throw new SQLException("Unsupported method supportsConvert");
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        throw new SQLException("Unsupported method supportsConvert");
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        throw new SQLException("Unsupported method supportsTableCorrelationNames");
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        throw new SQLException("Unsupported method supportsDifferentTableCorrelationNames");
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        throw new SQLException("Unsupported method supportsExpressionsInOrderBy");
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        throw new SQLException("Unsupported method supportsOrderByUnrelated");
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        throw new SQLException("Unsupported method supportsGroupBy");
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        throw new SQLException("Unsupported method supportsGroupByUnrelated");
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        throw new SQLException("Unsupported method supportsGroupByBeyondSelect");
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        throw new SQLException("Unsupported method supportsLikeEscapeClause");
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        throw new SQLException("Unsupported method supportsMultipleResultSets");
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        throw new SQLException("Unsupported method supportsMultipleTransactions");
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        throw new SQLException("Unsupported method supportsNonNullableColumns");
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        throw new SQLException("Unsupported method supportsMinimumSQLGrammar");
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        throw new SQLException("Unsupported method supportsCoreSQLGrammar");
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        throw new SQLException("Unsupported method supportsExtendedSQLGrammar");
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        throw new SQLException("Unsupported method supportsANSI92EntryLevelSQL");
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        throw new SQLException("Unsupported method supportsANSI92IntermediateSQL");
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        throw new SQLException("Unsupported method supportsANSI92FullSQL");
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        throw new SQLException("Unsupported method supportsIntegrityEnhancementFacility");
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        throw new SQLException("Unsupported method supportsOuterJoins");
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        throw new SQLException("Unsupported method supportsFullOuterJoins");
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        throw new SQLException("Unsupported method supportsLimitedOuterJoins");
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        throw new SQLException("Unsupported method getSchemaTerm");
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        throw new SQLException("Unsupported method getProcedureTerm");
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        throw new SQLException("Unsupported method getCatalogTerm");
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        throw new SQLException("Unsupported method isCatalogAtStart");
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        throw new SQLException("Unsupported method getCatalogSeparator");
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        throw new SQLException("Unsupported method supportsSchemasInDataManipulation");
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        throw new SQLException("Unsupported method supportsSchemasInProcedureCalls");
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        throw new SQLException("Unsupported method supportsSchemasInTableDefinitions");
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        throw new SQLException("Unsupported method supportsSchemasInIndexDefinitions");
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        throw new SQLException("Unsupported method supportsSchemasInPrivilegeDefinitions");
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        throw new SQLException("Unsupported method supportsCatalogsInDataManipulation");
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        throw new SQLException("Unsupported method supportsCatalogsInProcedureCalls");
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        throw new SQLException("Unsupported method supportsCatalogsInTableDefinitions");
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        throw new SQLException("Unsupported method supportsCatalogsInIndexDefinitions");
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        throw new SQLException("Unsupported method supportsCatalogsInPrivilegeDefinitions");
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        throw new SQLException("Unsupported method supportsPositionedDelete");
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        throw new SQLException("Unsupported method supportsPositionedUpdate");
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        throw new SQLException("Unsupported method supportsSelectForUpdate");
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        throw new SQLException("Unsupported method supportsStoredProcedures");
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        throw new SQLException("Unsupported method supportsSubqueriesInComparisons");
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        throw new SQLException("Unsupported method supportsSubqueriesInExists");
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        throw new SQLException("Unsupported method supportsSubqueriesInIns");
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        throw new SQLException("Unsupported method supportsSubqueriesInQuantifieds");
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        throw new SQLException("Unsupported method supportsCorrelatedSubqueries");
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        throw new SQLException("Unsupported method supportsUnion");
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        throw new SQLException("Unsupported method supportsUnionAll");
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        throw new SQLException("Unsupported method supportsOpenCursorsAcrossCommit");
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        throw new SQLException("Unsupported method supportsOpenCursorsAcrossRollback");
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        throw new SQLException("Unsupported method supportsOpenStatementsAcrossCommit");
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        throw new SQLException("Unsupported method supportsOpenStatementsAcrossRollback");
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        throw new SQLException("Unsupported method getMaxBinaryLiteralLength");
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        throw new SQLException("Unsupported method getMaxCharLiteralLength");
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        throw new SQLException("Unsupported method getMaxColumnNameLength");
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        throw new SQLException("Unsupported method getMaxColumnsInGroupBy");
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        throw new SQLException("Unsupported method getMaxColumnsInIndex");
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        throw new SQLException("Unsupported method getMaxColumnsInOrderBy");
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        throw new SQLException("Unsupported method getMaxColumnsInSelect");
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        throw new SQLException("Unsupported method getMaxColumnsInTable");
    }

    @Override
    public int getMaxConnections() throws SQLException {
        throw new SQLException("Unsupported method getMaxConnections");
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        throw new SQLException("Unsupported method getMaxCursorNameLength");
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        throw new SQLException("Unsupported method getMaxIndexLength");
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        throw new SQLException("Unsupported method getMaxSchemaNameLength");
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        throw new SQLException("Unsupported method getMaxProcedureNameLength");
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        throw new SQLException("Unsupported method getMaxCatalogNameLength");
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        throw new SQLException("Unsupported method getMaxRowSize");
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        throw new SQLException("Unsupported method doesMaxRowSizeIncludeBlobs");
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        throw new SQLException("Unsupported method getMaxStatementLength");
    }

    @Override
    public int getMaxStatements() throws SQLException {
        throw new SQLException("Unsupported method getMaxStatements");
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        throw new SQLException("Unsupported method getMaxTableNameLength");
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        throw new SQLException("Unsupported method getMaxTablesInSelect");
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        throw new SQLException("Unsupported method getMaxUserNameLength");
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        throw new SQLException("Unsupported method getDefaultTransactionIsolation");
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        throw new SQLException("Unsupported method supportsTransactions");
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        throw new SQLException("Unsupported method supportsTransactionIsolationLevel");
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        throw new SQLException("Unsupported method supportsDataDefinitionAndDataManipulationTransactions");
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        throw new SQLException("Unsupported method supportsDataManipulationTransactionsOnly");
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        throw new SQLException("Unsupported method dataDefinitionCausesTransactionCommit");
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        throw new SQLException("Unsupported method dataDefinitionIgnoredInTransactions");
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        throw new SQLException("Unsupported method getProcedures");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Unsupported method getProcedureColumns");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        throw new SQLException("Unsupported method getTables");
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        throw new SQLException("Unsupported method getSchemas");
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        throw new SQLException("Unsupported method getCatalogs");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        throw new SQLException("Unsupported method getTableTypes");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Unsupported method getColumns");
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        throw new SQLException("Unsupported method getColumnPrivileges");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLException("Unsupported method getTablePrivileges");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        throw new SQLException("Unsupported method getBestRowIdentifier");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Unsupported method getVersionColumns");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Unsupported method getPrimaryKeys");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Unsupported method getImportedKeys");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLException("Unsupported method getExportedKeys");
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        throw new SQLException("Unsupported method getCrossReference");
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        throw new SQLException("Unsupported method getTypeInfo");
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        throw new SQLException("Unsupported method getIndexInfo");
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        throw new SQLException("Unsupported method supportsResultSetType");
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        throw new SQLException("Unsupported method supportsResultSetConcurrency");
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        throw new SQLException("Unsupported method ownUpdatesAreVisible");
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        throw new SQLException("Unsupported method ownDeletesAreVisible");
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        throw new SQLException("Unsupported method ownInsertsAreVisible");
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        throw new SQLException("Unsupported method othersUpdatesAreVisible");
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        throw new SQLException("Unsupported method othersDeletesAreVisible");
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        throw new SQLException("Unsupported method othersInsertsAreVisible");
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        throw new SQLException("Unsupported method updatesAreDetected");
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        throw new SQLException("Unsupported method deletesAreDetected");
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        throw new SQLException("Unsupported method insertsAreDetected");
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        throw new SQLException("Unsupported method supportsBatchUpdates");
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        throw new SQLException("Unsupported method getUDTs");
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new SQLException("Unsupported method getConnection");
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        throw new SQLException("Unsupported method supportsSavepoints");
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        throw new SQLException("Unsupported method supportsNamedParameters");
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        throw new SQLException("Unsupported method supportsMultipleOpenResults");
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        throw new SQLException("Unsupported method supportsGetGeneratedKeys");
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        throw new SQLException("Unsupported method getSuperTypes");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLException("Unsupported method getSuperTables");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        throw new SQLException("Unsupported method getAttributes");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        throw new SQLException("Unsupported method supportsResultSetHoldability");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLException("Unsupported method getResultSetHoldability");
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        throw new SQLException("Unsupported method getDatabaseMajorVersion");
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        throw new SQLException("Unsupported method getDatabaseMinorVersion");
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        throw new SQLException("Unsupported method getJDBCMajorVersion");
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        throw new SQLException("Unsupported method getJDBCMinorVersion");
    }

    @Override
    public int getSQLStateType() throws SQLException {
        throw new SQLException("Unsupported method getSQLStateType");
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        throw new SQLException("Unsupported method locatorsUpdateCopy");
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        throw new SQLException("Unsupported method supportsStatementPooling");
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLException("Unsupported method getRowIdLifetime");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw new SQLException("Unsupported method getSchemas");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        throw new SQLException("Unsupported method supportsStoredFunctionsUsingCallSyntax");
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw new SQLException("Unsupported method autoCommitFailureClosesAllResultSets");
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLException("Unsupported method getClientInfoProperties");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        throw new SQLException("Unsupported method getFunctions");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Unsupported method getFunctionColumns");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLException("Unsupported method getPseudoColumns");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw new SQLException("Unsupported method generatedKeyAlwaysReturned");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Unsupported method");
    }
}
