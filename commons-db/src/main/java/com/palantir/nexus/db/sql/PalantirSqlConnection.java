package com.palantir.nexus.db.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.palantir.exception.PalantirInterruptedException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.SQLString.RegisteredSQLString;

public interface PalantirSqlConnection {
    public void executeUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException;
    public int executeUnregisteredQueryCountRows(String sql, Object... vs) throws PalantirSqlException;
    public PreparedStatement execute(String key, Object... vs) throws PalantirSqlException;
    public PreparedStatement execute(RegisteredSQLString sql, Object... vs) throws PalantirSqlException;
    public int executeCountRows(String key, Object... vs) throws PalantirSqlException;
    public int executeCountRows(RegisteredSQLString sql, Object... vs) throws PalantirSqlException;
    public long selectCount(String tableName) throws PalantirSqlException, PalantirInterruptedException;
    public long selectCount(String tableName, String whereClause, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public boolean selectExistsUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public boolean selectExists(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public boolean selectExists(final RegisteredSQLString sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public int selectIntegerUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public int selectInteger(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public int selectInteger(final RegisteredSQLString sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    /**
     * Returns the long value of the first field selected given a query.
     * <p>
     * If the value of the first field is null then 0 will be returned in prod,
     * but an assert will be thrown in test.
     *
     * @deprecated Use selectLongWithDefaultUnregisteredQuery to control behavior when no or NULL results are found.
     */
    @Deprecated
    public long selectLongUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    /**
     * Returns the long value of the first field selected given a query.  If
     * no rows are returned, will throw SQLException.
     * <p>
     * If the value of the first field is null then 0 will be returned in prod,
     * but an assert will be thrown in test.
     */
    public long selectLong(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    /**
     * Returns the long value of the first field selected given a query.  If
     * no rows are returned, will throw SQLException.
     * <p>
     * If the value of the first field is null then 0 will be returned in prod,
     * but an assert will be thrown in test.
     */
    public long selectLong(RegisteredSQLString sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    /**
     * Returns the long value of the first field selected given a query If
     * no rows are returned, will return defaultVal.
     * <p>
     * If the value of the first field is null, then defaultVal will be returned. This means
     * that if defaultVal is non-null, then this method won't return null.
     */
    public Long selectLongWithDefaultUnregisteredQuery(String sql, Long defaultVal, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    /**
     * Returns the long value of the first field selected given a query If
     * no rows are returned, will return defaultVal.
     * <p>
     * If the value of the first field is null, then defaultVal will be returned. This means
     * that if defaultVal is non-null, then this method won't return null.
     */
    public Long selectLongWithDefault(String key, Long defaultVal, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    /**
     * Returns the long value of the first field selected given a query If
     * no rows are returned, will return defaultVal.
     * <p>
     * If the value of the first field is null, then defaultVal will be returned. This means
     * that if defaultVal is non-null, then this method won't return null.
     */
    public Long selectLongWithDefault(RegisteredSQLString sql, Long defaultVal, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    public AgnosticLightResultSet selectLightResultSetUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public AgnosticLightResultSet selectLightResultSet(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public AgnosticLightResultSet selectLightResultSet(RegisteredSQLString sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public AgnosticResultSet selectResultSetUnregisteredQuery(String sql, Object...vs) throws PalantirSqlException, PalantirInterruptedException;
    public AgnosticResultSet selectResultSet(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public AgnosticResultSet selectResultSet(RegisteredSQLString sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public boolean updateUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException;
    public boolean update(String key, Object... vs) throws PalantirSqlException;
    public boolean update(final RegisteredSQLString sql, Object... vs) throws PalantirSqlException;
    public int updateCountRowsUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException;
    public int updateCountRows(String key, Object... vs) throws PalantirSqlException;
    public int updateCountRows(RegisteredSQLString sql, Object... vs) throws PalantirSqlException;
    public void updateManyUnregisteredQuery(String sql) throws PalantirSqlException;
    public void updateManyUnregisteredQuery(String sql, Iterable<Object[]> list) throws PalantirSqlException;
    public void updateMany(String key) throws PalantirSqlException;
    public void updateMany(String key, Iterable<Object[]> list) throws PalantirSqlException;
    public void updateMany(RegisteredSQLString sql) throws PalantirSqlException;
    public void updateMany(RegisteredSQLString sql, Iterable<Object[]> list) throws PalantirSqlException;
    public boolean insertOneUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException;
    public boolean insertOne(String key, Object... vs) throws PalantirSqlException;
    public boolean insertOne(RegisteredSQLString sql, Object... vs) throws PalantirSqlException;
    public int insertOneCountRowsUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException;
    public int insertOneCountRows(String key, Object... vs) throws PalantirSqlException;
    public int insertOneCountRows(final RegisteredSQLString sql, final Object... vs) throws PalantirSqlException;
    public boolean insertManyUnregisteredQuery(String sql, Iterable<Object[]> list) throws PalantirSqlException;
    public boolean insertMany(String key, Iterable<Object[]> list) throws PalantirSqlException;
    public boolean insertMany(RegisteredSQLString sql, Iterable<Object[]> list) throws PalantirSqlException;
    /**
     * Clears out any existing rows in PT_TEMP_IDS and inserts the caller's list of temp ids.
     * All users of this temp table should use this function, so that multiple uses within the
     * same transaction do not interfere with each other.
     *
     * The main use case is that the caller has a large list of ids that would otherwise become part
     * of a "AND table.column in (?,?,?,...)", potentially running into Oracle's limit on elements in
     * a list unless the query is broken up.  Instead, you can add all of the ids in a single shot to this
     * temp table, and then convert your query into a join.  Apparently, this is offers a performance improvement.
     *
     * Note: This does NOT need to run within a transaction. Each {@link java.sql.Connection} gets its own
     *       temp table.
     *
     * @param tempIds collection of Longs to be inserted into PT_TEMP_IDS
     * @throws PalantirSqlException
     */
    public void loadTempIds(Iterable<Long> tempIds) throws PalantirSqlException;

    /**
     * Create clears out the contents of the temp table with the name provided and replaces
     * it with the numbers passed in via 'tempIds'
     *
     * @param tempIds collection of Longs to be inserted into specified temp table
     * @param tableName name of temporary table which will be cleared out and inserted with data
     * @throws PalantirSqlException
     */
    public void loadTempIds(Iterable<Long> tempIds, String tableName) throws PalantirSqlException;
    public void deleteIdsFromTempIds(Collection<Long> ids) throws PalantirSqlException;
    public void deleteIdsFromTempIdsForTable(Collection<Long> ids, String tempTable) throws PalantirSqlException;
    public void loadTempIdPairsIntoEight(Map<Long, Long> idToField1) throws PalantirSqlException;
    public void loadEightFieldTempIds(Iterable<Object[]> args) throws PalantirSqlException;
    public void loadIdKeyPairTempIds(Iterable<Object[]> args) throws PalantirSqlException;
    public void loadTempExternalIds(Iterable<Object[]> externalIds) throws PalantirSqlException;
    public void loadThreeFieldTempIds(Iterable<Object[]> args) throws PalantirSqlException;
    public void loadTempIdPairsIntoIdKeyTuples(Map<Long, Long> idToField1) throws PalantirSqlException;
    public void loadIdKeyTuplesTempIds(List<Object[]> tempRows) throws PalantirSqlException;
    public void loadKeysTempIds(Iterable<String> args) throws PalantirSqlException;
    public void clearTempTable(String tempTable) throws PalantirSqlException;

    /**
     * This will return the current time.  This is better than {@link System#currentTimeMillis()} because it will take
     * drift into account if this host has a different time than the database host.  Using this method everywhere will
     * ensure that all wall clock time will be drawn from a consistent clock (the DB node).
     *
     * @return the current wall clock time.
     */
    public long getTimestamp();

    public Connection getUnderlyingConnection() throws PalantirSqlException;
}
