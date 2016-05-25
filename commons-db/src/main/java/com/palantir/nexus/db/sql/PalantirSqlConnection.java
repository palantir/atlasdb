/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.nexus.db.sql;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.palantir.exception.PalantirInterruptedException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.SQLString.RegisteredSQLString;

public interface PalantirSqlConnection extends SqlConnection {
    int executeUnregisteredQueryCountRows(String sql, Object... vs) throws PalantirSqlException;
    int executeCountRows(String key, Object... vs) throws PalantirSqlException;
    int executeCountRows(RegisteredSQLString sql, Object... vs) throws PalantirSqlException;
    int updateCountRowsUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException;
    int insertOneCountRowsUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException;
    int insertOneCountRows(String key, Object... vs) throws PalantirSqlException;
    int insertOneCountRows(final RegisteredSQLString sql, final Object... vs) throws PalantirSqlException;

    /**
     * Returns the long value of the first field selected given a query.
     * <p>
     * If the value of the first field is null then 0 will be returned in prod,
     * but an assert will be thrown in test.
     *
     * @deprecated Use selectLongWithDefaultUnregisteredQuery to control behavior when no or NULL results are found.
     */
    @Deprecated
    long selectLongUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    /**
     * Returns the long value of the first field selected given a query.  If
     * no rows are returned, will throw SQLException.
     * <p>
     * If the value of the first field is null then 0 will be returned in prod,
     * but an assert will be thrown in test.
     */
    long selectLong(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    /**
     * Returns the long value of the first field selected given a query.  If
     * no rows are returned, will throw SQLException.
     * <p>
     * If the value of the first field is null then 0 will be returned in prod,
     * but an assert will be thrown in test.
     */
    long selectLong(RegisteredSQLString sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    /**
     * Returns the long value of the first field selected given a query If
     * no rows are returned, will return defaultVal.
     * <p>
     * If the value of the first field is null, then defaultVal will be returned. This means
     * that if defaultVal is non-null, then this method won't return null.
     */
    Long selectLongWithDefaultUnregisteredQuery(String sql, Long defaultVal, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    /**
     * Returns the long value of the first field selected given a query If
     * no rows are returned, will return defaultVal.
     * <p>
     * If the value of the first field is null, then defaultVal will be returned. This means
     * that if defaultVal is non-null, then this method won't return null.
     */
    Long selectLongWithDefault(String key, Long defaultVal, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    /**
     * Returns the long value of the first field selected given a query If
     * no rows are returned, will return defaultVal.
     * <p>
     * If the value of the first field is null, then defaultVal will be returned. This means
     * that if defaultVal is non-null, then this method won't return null.
     */
    Long selectLongWithDefault(RegisteredSQLString sql, Long defaultVal, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

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
    void loadTempIds(Iterable<Long> tempIds) throws PalantirSqlException;

    /**
     * Create clears out the contents of the temp table with the name provided and replaces
     * it with the numbers passed in via 'tempIds'
     *
     * @param tempIds collection of Longs to be inserted into specified temp table
     * @param tableName name of temporary table which will be cleared out and inserted with data
     * @throws PalantirSqlException
     */
    void loadTempIds(Iterable<Long> tempIds, String tableName) throws PalantirSqlException;
    void deleteIdsFromTempIds(Collection<Long> ids) throws PalantirSqlException;
    void deleteIdsFromTempIdsForTable(Collection<Long> ids, String tempTable) throws PalantirSqlException;
    void loadTempIdPairsIntoEight(Map<Long, Long> idToField1) throws PalantirSqlException;
    void loadEightFieldTempIds(Iterable<Object[]> args) throws PalantirSqlException;
    void loadIdKeyPairTempIds(Iterable<Object[]> args) throws PalantirSqlException;
    void loadTempExternalIds(Iterable<Object[]> externalIds) throws PalantirSqlException;
    void loadThreeFieldTempIds(Iterable<Object[]> args) throws PalantirSqlException;
    void loadTempIdPairsIntoIdKeyTuples(Map<Long, Long> idToField1) throws PalantirSqlException;
    void loadIdKeyTuplesTempIds(List<Object[]> tempRows) throws PalantirSqlException;
    void loadKeysTempIds(Iterable<String> args) throws PalantirSqlException;
    void clearTempTable(String tempTable) throws PalantirSqlException;

    /**
     * This will return the current time.  This is better than {@link System#currentTimeMillis()} because it will take
     * drift into account if this host has a different time than the database host.  Using this method everywhere will
     * ensure that all wall clock time will be drawn from a consistent clock (the DB node).
     *
     * @return the current wall clock time.
     */
    long getTimestamp();
}
