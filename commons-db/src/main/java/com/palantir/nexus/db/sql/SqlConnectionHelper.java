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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.Validate;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.common.collect.IterableView;
import com.palantir.exception.PalantirInterruptedException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.BasicSQL.AutoClose;
import com.palantir.nexus.db.sql.BasicSQLString.FinalSQLString;
import com.palantir.nexus.db.sql.SQLString.RegisteredSQLString;
import com.palantir.sql.PreparedStatements;
import com.palantir.util.TextUtils;
import com.palantir.util.sql.SqlStats;

/**
 * Helper methods for {@link PalantirSqlConnection} implementations. These were originally
 * deprecated static methods on {@link SQL}.
 *
 * @author jweel
 */
public final class SqlConnectionHelper {

    private static final Logger log = LogManager.getLogger(SqlConnectionHelper.class);
    private static final int TEMP_IDS_LOGGING_THRESHOLD = 100000;

    private final BasicSQL basicSql;

    public SqlConnectionHelper(BasicSQL basicSql) {
        this.basicSql = basicSql;
    }

    void executeUnregisteredQuery(Connection c, String sql, Object... vs)
            throws PalantirSqlException {
        basicSql.execute(c, SQLString.getUnregisteredQuery(sql), vs, AutoClose.TRUE);
    }

    int executeUnregisteredQueryCountRows(Connection c, String sql, Object... vs)
            throws PalantirSqlException {
        return executeCountRows(c, SQLString.getUnregisteredQuery(sql), vs);
    }

    PreparedStatement execute(Connection c, String key, Object... vs) throws PalantirSqlException {
        return basicSql.execute(c, SQLString.getByKey(key, c), vs, AutoClose.TRUE);
    }

    public int executeCountRows(Connection c, String key, Object... vs) throws PalantirSqlException {
        return executeCountRows(c, SQLString.getByKey(key, c), vs);
    }

    private int executeCountRows(Connection c, FinalSQLString sql, Object... vs)
            throws PalantirSqlException {
        PreparedStatement ps = null;
        try {
            ps = basicSql.execute(c, sql, vs, AutoClose.FALSE);
            return PreparedStatements.getUpdateCount(ps);
        } finally {
            BasicSQL.closeSilently(ps);
        }
    }

    /**
     * Returns true if at least one row comes back for the provided Otherwise, returns false.
     *
     * @param c
     * @param sql
     * @param vs
     * @return
     * @throws PalantirSqlException
     */

    long selectCount(Connection c, String tableName) throws PalantirSqlException,
            PalantirInterruptedException {
        return selectCount(c, tableName, null, new Object[] {});
    }

    long selectCount(Connection c, String tableName, String whereClause, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        String sql = "SELECT count(*) from " + tableName; //$NON-NLS-1$
        if (whereClause != null) {
            sql += " WHERE " + whereClause; //$NON-NLS-1$
        }
        return selectLongUnregisteredQuery(c, sql, vs);
    }

    /**
     * Returns true if at least one row comes back for the provided Otherwise, returns false.
     *
     * @param c
     * @param sql
     * @param vs
     * @return
     * @throws PalantirSqlException
     */

    boolean selectExistsUnregisteredQuery(Connection c, String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return basicSql.selectExistsInternal(c, SQLString.getUnregisteredQuery(sql), vs);
    }

    boolean selectExists(Connection c, String key, Object... vs) throws PalantirSqlException,
            PalantirInterruptedException {
        return basicSql.selectExistsInternal(c, SQLString.getByKey(key, c), vs);
    }

    /**
     * Returns the integer value of the first field selected given a query. If no rows are returned,
     * will throw SQLException.
     * <p>
     * If the first field is null, then 0 will be returned.
     */

    int selectIntegerUnregisteredQuery(Connection c, String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return basicSql.selectIntegerInternal(c, SQLString.getUnregisteredQuery(sql), vs);
    }

    int selectInteger(Connection c, String key, Object... vs) throws PalantirSqlException,
            PalantirInterruptedException {
        return basicSql.selectIntegerInternal(c, SQLString.getByKey(key, c), vs);
    }

    /**
     * Returns the long value of the first field selected given a query. If no rows are returned,
     * will throw SQLException.
     * <p>
     * If the value of the first field is null then 0 will be returned in prod, but an assert will
     * be thrown in test.
     */

    long selectLongUnregisteredQuery(Connection c, String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return basicSql.selectLongInternal(c, SQLString.getUnregisteredQuery(sql), vs, null, true);
    }

    /**
     * Returns the long value of the first field selected given a query. If no rows are returned,
     * will throw SQLException.
     * <p>
     * If the value of the first field is null then 0 will be returned in prod, but an assert will
     * be thrown in test.
     */

    long selectLong(Connection c, String key, Object... vs) throws PalantirSqlException,
            PalantirInterruptedException {
        return basicSql.selectLongInternal(c, SQLString.getByKey(key, c), vs, null, true);
    }

    /**
     * Returns the long value of the first field selected given a query. If no rows are returned,
     * will throw SQLException.
     * <p>
     * If the value of the first field is null then 0 will be returned in prod, but an assert will
     * be thrown in test.
     */

    long selectLong(Connection c, RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return selectLong(c, sql.getKey(), vs);
    }

    /**
     * Returns the long value of the first field selected given a query If no rows are returned,
     * will return defaultVal.
     * <p>
     * If the value of the first field is null, then defaultVal will be returned. This means that if
     * defaultVal is non-null, then this method won't return null.
     */

    Long selectLongWithDefaultUnregisteredQuery(Connection c,
                                                String sql,
                                                Long defaultVal,
                                                Object... vs) throws PalantirSqlException,
            PalantirInterruptedException {
        return basicSql.selectLongInternal(
                c,
                SQLString.getUnregisteredQuery(sql),
                vs,
                defaultVal,
                false);
    }

    /**
     * Returns the long value of the first field selected given a query If no rows are returned,
     * will return defaultVal.
     * <p>
     * If the value of the first field is null, then defaultVal will be returned. This means that if
     * defaultVal is non-null, then this method won't return null.
     */

    Long selectLongWithDefault(Connection c, String key, Long defaultVal, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return basicSql.selectLongInternal(c, SQLString.getByKey(key, c), vs, defaultVal, false);
    }

    AgnosticLightResultSet selectLightResultSetUnregisteredQuery(Connection c,
                                                                 String sql,
                                                                 Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return basicSql.selectLightResultSetSpecifyingDBType(
                c,
                SQLString.getUnregisteredQuery(sql),
                vs,
                DBType.getTypeFromConnection(c));
    }

    AgnosticLightResultSet selectLightResultSet(Connection c, String key, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return selectLightResultSet(c, SQLString.getByKey(key, c), vs);
    }

    AgnosticLightResultSet selectLightResultSet(Connection c, FinalSQLString finalSql, Object... vs)
            throws PalantirSqlException {
        return basicSql.selectLightResultSetSpecifyingDBType(
                c,
                finalSql,
                vs,
                DBType.getTypeFromConnection(c));
    }

    AgnosticLightResultSet selectLightResultSet(Connection c, RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        DBType dbType = DBType.getTypeFromConnection(c);
        return basicSql.selectLightResultSetSpecifyingDBType(
                c,
                SQLString.getByKey(sql.getKey(), dbType),
                vs,
                dbType);
    }

    AgnosticResultSet selectResultSetUnregisteredQuery(Connection c, String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return basicSql.selectResultSetSpecifyingDBType(
                c,
                SQLString.getUnregisteredQuery(sql),
                vs,
                DBType.getTypeFromConnection(c));
    }

    AgnosticResultSet selectResultSet(Connection c, String key, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        DBType dbType = DBType.getTypeFromConnection(c);
        return basicSql.selectResultSetSpecifyingDBType(
                c,
                SQLString.getByKey(key, dbType),
                vs,
                dbType);
    }

    AgnosticResultSet selectResultSet(Connection c, RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return selectResultSet(c, sql.getKey(), vs);
    }

    boolean updateUnregisteredQuery(Connection c, String sql, Object... vs)
            throws PalantirSqlException {
        basicSql.updateInternal(c, SQLString.getUnregisteredQuery(sql), vs, AutoClose.TRUE);
        return true;
    }

    boolean update(Connection c, String key, Object... vs) throws PalantirSqlException {
        basicSql.updateInternal(c, SQLString.getByKey(key, c), vs, AutoClose.TRUE);
        return true;
    }

    boolean update(Connection c, RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        return update(c, sql.getKey(), vs);
    }

    int updateCountRowsUnregisteredQuery(Connection c, String sql, Object... vs)
            throws PalantirSqlException {
        return basicSql.updateCountRowsInternal(c, SQLString.getUnregisteredQuery(sql), vs);
    }

    int updateCountRows(Connection c, String key, Object... vs) throws PalantirSqlException {
        return basicSql.updateCountRowsInternal(c, SQLString.getByKey(key, c), vs);
    }

    int updateCountRows(Connection c, RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException {
        return updateCountRows(c, sql.getKey(), vs);
    }

    void updateManyUnregisteredQuery(Connection c, String sql) throws PalantirSqlException {
        updateManyUnregisteredQuery(c, sql, ImmutableList.<Object[]> of());
    }

    void updateManyUnregisteredQuery(Connection c, String sql, Iterable<Object[]> list)
            throws PalantirSqlException {
        basicSql.updateMany(
                c,
                SQLString.getUnregisteredQuery(sql),
                Iterables.toArray(list, Object[].class));
    }

    void updateMany(Connection c, String key) throws PalantirSqlException {
        updateMany(c, key, ImmutableList.<Object[]> of());
    }

    void updateMany(Connection c, String key, Iterable<Object[]> list) throws PalantirSqlException {
        basicSql.updateMany(c, SQLString.getByKey(key, c), Iterables.toArray(list, Object[].class));
    }

    void updateMany(Connection c, RegisteredSQLString sql) throws PalantirSqlException {
        updateMany(c, sql.getKey(), ImmutableList.<Object[]> of());
    }

    void updateMany(Connection c, RegisteredSQLString sql, Iterable<Object[]> list)
            throws PalantirSqlException {
        updateMany(c, sql.getKey(), list);
    }

    boolean insertOneUnregisteredQuery(Connection c, String sql, Object... vs)
            throws PalantirSqlException {
        final int updated = insertOneCountRowsUnregisteredQuery(c, sql, vs);
        assert updated == 1 : "expected 1 update, got : " + updated; //$NON-NLS-1$
        return true;
    }

    boolean insertOne(Connection c, String key, Object... vs) throws PalantirSqlException {
        final int updated = insertOneCountRows(c, key, vs);
        assert updated == 1 : "expected 1 update, got : " + updated; //$NON-NLS-1$
        return true;
    }

    boolean insertOne(Connection c, RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException {
        return insertOne(c, sql.getKey(), vs);
    }

    int insertOneCountRowsUnregisteredQuery(Connection c, String sql, Object... vs)
            throws PalantirSqlException {
        return basicSql.insertOneCountRowsInternal(c, SQLString.getUnregisteredQuery(sql), vs);
    }

    int insertOneCountRows(Connection c, String key, Object... vs) throws PalantirSqlException {
        return basicSql.insertOneCountRowsInternal(c, SQLString.getByKey(key, c), vs);
    }

    int insertOneCountRows(Connection c, RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException {
        return insertOneCountRows(c, sql.getKey(), vs);
    }

    boolean insertManyUnregisteredQuery(Connection c, String sql, Iterable<Object[]> list)
            throws PalantirSqlException {
        return basicSql.insertMany(
                c,
                SQLString.getUnregisteredQuery(sql),
                Iterables.toArray(list, Object[].class));
    }

    boolean insertMany(Connection c, String key, Iterable<Object[]> list)
            throws PalantirSqlException {
        return basicSql.insertMany(
                c,
                SQLString.getByKey(key, c),
                Iterables.toArray(list, Object[].class));
    }

    boolean insertMany(Connection c, RegisteredSQLString sql, Iterable<Object[]> list)
            throws PalantirSqlException {
        return insertMany(c, sql.getKey(), list);
    }

    private final String SQL_TRUNCATE_PT_TEMP_IDS = " TRUNCATE TABLE /* SQL_TRUNCATE_PT_TEMP_IDS */ {0} "; //$NON-NLS-1$

    private final String SQL_DELETE_PT_TEMP_IDS = " DELETE /* SQL_DELETE_PT_TEMP_IDS */ FROM {0} "; //$NON-NLS-1$

    private final String SQL_DELETE_IDS_FROM_PT_TEMPS_IDS = " DELETE /* SQL_DELETE_IDS_FROM_PT_TEMPS_IDS */ FROM {0} WHERE id = ? "; //$NON-NLS-1$

    // for large tables this may be faster, and if we know that pt_temp_ids is large, then we
    // may want to use it. However, we've found that DELETE is faster on Oracle.
    // NOTE: this query cannot be used with HSQLDB
    // private final String SQL_TRUNCATE_PT_TEMP_IDS =
    // " TRUNCATE /* SQL_TRUNCATE_PT_TEMP_IDS */ TABLE {0} ";

    private final String SQL_INSERT_INTO_PT_TEMP_IDS = " INSERT /*+ APPEND */ INTO /* SQL_INSERT_INTO_PT_TEMP_IDS */ {0} (id) values (?) "; //$NON-NLS-1$

    static {
        SQLString.registerQuery("SQL_INSERT_INTO_PT_TEMP_IDS_EIGHT_FIELDS", //$NON-NLS-1$
                " INSERT /*+ APPEND */ " + //$NON-NLS-1$
                        " INTO "
                        + //$NON-NLS-1$
                        " PT_TEMP_IDS_EIGHT_FIELDS "
                        + //$NON-NLS-1$
                        "    (id, field1, field2, field3, field4, field5, field6, field7, field8) "
                        + //$NON-NLS-1$
                        " VALUES (?,?,?,?,?,?,?,?,?) "); //$NON-NLS-1$
        SQLString.registerQuery("SQL_INSERT_INTO_PT_TEMP_IDS_ID_KEY_PAIRS", //$NON-NLS-1$
                " INSERT /*+ APPEND */ " + //$NON-NLS-1$
                        " INTO " + //$NON-NLS-1$
                        " PT_TEMP_IDS_ID_KEY_PAIRS " + //$NON-NLS-1$
                        "    (id, key) " + //$NON-NLS-1$
                        " VALUES (?,?) "); //$NON-NLS-1$
        SQLString.registerQuery("SQL_INSERT_INTO_PT_TEMP_IDS_KEYS", //$NON-NLS-1$
                " INSERT /*+ APPEND */ " + //$NON-NLS-1$
                        " INTO " + //$NON-NLS-1$
                        " PT_TEMP_IDS_KEYS " + //$NON-NLS-1$
                        "    (key) " + //$NON-NLS-1$
                        " VALUES (?) "); //$NON-NLS-1$
        SQLString.registerQuery("SQL_INSERT_INTO_PT_TEMP_THREE_IDS", //$NON-NLS-1$
                " INSERT /*+ APPEND */ " + //$NON-NLS-1$
                " INTO " + //$NON-NLS-1$
                " PT_TEMP_THREE_IDS " + //$NON-NLS-1$
                "    (id1, id2, id3) " + //$NON-NLS-1$
                " VALUES (?,?,?) "); //$NON-NLS-1$
        SQLString.registerQuery("SQL_INSERT_INTO_PT_TEMP_EXTERNAL_IDS", //$NON-NLS-1$
                " INSERT /*+ APPEND */ " + //$NON-NLS-1$
                        " INTO " + //$NON-NLS-1$
                        " PT_TEMP_EXTERNAL_IDS" + //$NON-NLS-1$
                        "    (original_system_id, original_system_nonce, original_object_id, " + //$NON-NLS-1$
                        "           original_object_id_extension, external_id_type," + //$NON-NLS-1$
                        "           field1, field2, field3, field4) " + //$NON-NLS-1$
                        " VALUES (" + BasicSQLUtils.nArguments(9) + ")"); //$NON-NLS-1$
    }

    private final RegisteredSQLString SQL_INSERT_INTO_PT_TEMP_IDS_ID_KEY_TUPLES = SQLString.registerQuery(
            "SQL_INSERT_INTO_PT_TEMP_IDS_ID_KEY_TUPLES", //$NON-NLS-1$
            " INSERT /*+ APPEND */ INTO " + //$NON-NLS-1$
                    " PT_TEMP_IDS_ID_KEY_TUPLES " + //$NON-NLS-1$
                    " (id, field1, field2, field3, field4, field5, field6, field7, field8, key) " + //$NON-NLS-1$
                    " VALUES (?,?,?,?,?,?,?,?,?,?) "); //$NON-NLS-1$

    private final Function<Long, Object[]> TEMP_ID_TO_ARGUMENT = new Function<Long, Object[]>() {
        @Override
        public Object[] apply(Long tempId) {
            return new Object[] { tempId };
        }
    };

    /**
     *
     * Create clears out the contents of the temp table with the name provided and replaces it with
     * the numbers passed in via 'tempIds'
     *
     * @param c the Connection
     * @param tempIds collection of Longs to be inserted into specified temp table
     * @param tableName name of temporary table which will be cleared out and inserted with data
     * @throws PalantirSqlException
     */

    void loadTempIds(Connection c, Iterable<Long> tempIds, ClearStyle clearStyle)
            throws PalantirSqlException {
        loadTempIds(c, tempIds, "PT_TEMP_IDS", clearStyle); //$NON-NLS-1$
    }

    void loadTempIds(Connection c, Iterable<Long> tempIds, String tableName, ClearStyle clearStyle)
            throws PalantirSqlException {
        // First, delete any existing rows from temp table
        clearTempTable(c, tableName, clearStyle);

        // Second, add the ids that the user passed in, if any where passed in
        if (tempIds != null && !Iterables.isEmpty(tempIds)) {
            int size = Iterables.size(tempIds);
            if (size > TEMP_IDS_LOGGING_THRESHOLD) {
                log.error("Putting " + size + " objects in temp ids; stack trace provided for debugging", new Exception());
            }
            Iterable<Object[]> args = idsToArguments(tempIds);
            String sqlInsert = TextUtils.format(SQL_INSERT_INTO_PT_TEMP_IDS, tableName);
            try {
                insertManyUnregisteredQuery(c, sqlInsert, args); // dynamic query
            } catch (PalantirSqlException e) {
                assert checkRepeatedInsert(e, c, tableName, tempIds, clearStyle);
                throw e;
            }
        }
    }

    /**
     * Check if the temp table has this exact set of tempIds already, for debugging purposes.
     * This should be behind an assert so it does not run in production.
     */
    private boolean checkRepeatedInsert(PalantirSqlException e, Connection c, String tableName, Iterable<Long> tempIds, ClearStyle clearStyle) {
        AgnosticResultSet results = selectResultSetUnregisteredQuery(c, "SELECT id FROM " + tableName);
        Set<Long> attempt = Sets.newHashSet(tempIds);
        Set<Long> current = Sets.newHashSet();
        String isInTransaction = transactionAsString(c);
        for (AgnosticResultRow row: results.rows()) {
            current.add(row.getLong("id"));
        }
        if (attempt.equals(current)) {
            String message = String.format("Tried to insert %s temp IDs into table %s, but that exact set of temp IDs was already there. (%s) (ClearStyle: %s)",
                    attempt.size(), tableName, isInTransaction, clearStyle);
            throw new RuntimeException(message, e);
        } else {
            Set<Long> onlyInAttempt = Sets.difference(attempt, current);
            Set<Long> onlyInCurrent = Sets.difference(current, attempt);
            String message = String.format(
                    "Tried to insert %s temp IDs into table %s, but %s temp IDs were already there.  %s were only in our attempt, while " +
                            "%s were only in the set already present. (%s) (ClearStyle: %s)",
                    attempt.size(), tableName, current.size(), onlyInAttempt.size(), onlyInCurrent.size(), isInTransaction, clearStyle);
            throw new RuntimeException(message, e);
        }
    }

    private String transactionAsString(Connection c) {
        String isInTransaction;
        try {
            if (c.getAutoCommit()) {
                isInTransaction = "Not in transaction";
            } else {
                isInTransaction = "In transaction";
            }
        } catch (SQLException sqle) {
            isInTransaction = "Unknown transaction status";
        }
        return isInTransaction;
    }

    private Iterable<Object[]> idsToArguments(Iterable<Long> tempIds) {
        // unless the input tempIds is a Set, we need to check that
        // the input collection of IDs contains no duplicate IDs
        Set<Long> uniqueTempIds;
        if (tempIds instanceof Set) {
            uniqueTempIds = (Set<Long>) tempIds;
        } else {
            uniqueTempIds = Sets.newHashSet(tempIds);
        }
        return Collections2.transform(uniqueTempIds, TEMP_ID_TO_ARGUMENT);
    }

    void deleteIdsFromTempIds(Connection c, Collection<Long> ids) throws PalantirSqlException {
        deleteIdsFromTempIdsForTable(c, ids, "pt_temp_ids"); //$NON-NLS-1$
    }

    void deleteIdsFromTempIdsForTable(Connection c, Collection<Long> ids, String tempTable)
            throws PalantirSqlException {
        Iterable<Object[]> args = IterableView.of(ids).transform(TEMP_ID_TO_ARGUMENT);
        String sql = TextUtils.format(SQL_DELETE_IDS_FROM_PT_TEMPS_IDS, tempTable);
        updateManyUnregisteredQuery(c, sql, args);
    }

    void loadEightFieldTempIds(Connection c, Iterable<Object[]> args, ClearStyle clearStyle)
            throws PalantirSqlException {
        clearTempTable(c, "PT_TEMP_IDS_EIGHT_FIELDS", clearStyle); //$NON-NLS-1$
        insertMany(c, "SQL_INSERT_INTO_PT_TEMP_IDS_EIGHT_FIELDS", args); //$NON-NLS-1$
    }

    void loadIdKeyPairTempIds(Connection c, Iterable<Object[]> args, ClearStyle clearStyle)
            throws PalantirSqlException {
        clearTempTable(c, "PT_TEMP_IDS_ID_KEY_PAIRS", clearStyle); //$NON-NLS-1$
        insertMany(c, "SQL_INSERT_INTO_PT_TEMP_IDS_ID_KEY_PAIRS", args); //$NON-NLS-1$
    }

    void loadTempExternalIds(Connection c, Iterable<Object[]> args, ClearStyle clearStyle)
            throws PalantirSqlException {
        clearTempTable(c, "PT_TEMP_EXTERNAL_IDS", clearStyle); //$NON-NLS-1$
        insertMany(c, "SQL_INSERT_INTO_PT_TEMP_EXTERNAL_IDS", args); //$NON-NLS-1$
    }

    void loadThreeFieldTempIds(Connection c,
                                      Iterable<Object[]> args,
                                      ClearStyle clearStyle) throws PalantirSqlException {
        clearTempTable(c, "PT_TEMP_THREE_IDS", clearStyle);
        insertMany(c, "SQL_INSERT_INTO_PT_TEMP_THREE_IDS", args);
    }

    void loadKeysTempIds(Connection c, Iterable<String> args, ClearStyle clearStyle)
            throws PalantirSqlException {
        clearTempTable(c, "PT_TEMP_IDS_KEYS", clearStyle); //$NON-NLS-1$
        List<Object[]> objArgs = Lists.newArrayList();
        for (String arg : args) {
            objArgs.add(new Object[] { arg });
        }
        insertMany(c, "SQL_INSERT_INTO_PT_TEMP_IDS_KEYS", objArgs); //$NON-NLS-1$
    }

    void clearTempTable(Connection c, String tempTable, ClearStyle clearStyle)
            throws PalantirSqlException {
        attemptToClearTempTable(c, tempTable, clearStyle);
        if (ClearStyle.TRUNCATE.equals(clearStyle)) {
            // We have noticed that TRUNCATE can fail silently on Oracle SE 11.2.0.4.0
            // So we must always confirm
            if (!isTableEmpty(c, tempTable)) {
                log.error(
                        String.format("On first attempt, did not clear temp table %s using style TRUNCATE.  Retrying.",
                        tempTable));
                attemptToClearTempTable(c, tempTable, clearStyle);
                if (!isTableEmpty(c, tempTable)) {
                    throw new RuntimeException(
                            String.format("On multiple attempts with clear style TRUNCATE, did not clear temp table %s",
                                    tempTable));
                }
            }
        } else {
            // We have never seen DELETE fail silently, so we will only confirm if assertions are enabled.
            assert verifyTableCleared(c, tempTable, clearStyle);
        }
    }

    private void attemptToClearTempTable(Connection c, String tempTable, ClearStyle clearStyle) {
        if (clearStyle == ClearStyle.DELETE) {
            clearTempTableUsingDelete(c, tempTable);
        } else if (clearStyle == ClearStyle.TRUNCATE) {
            clearTempTableUsingTruncate(c, tempTable);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private boolean isTableEmpty(Connection c, String table) {
        return !selectExistsUnregisteredQuery(c, "SELECT 1 from " + table);
    }

    /**
     * If the table was not cleared, try to diagnose the reason and throw an informative error message.
     * In the future, we may want to allow the thread to continue executing if we eventually clear the temp table,
     * but for now we will always throw if the initial attempt failed.
     */
    private boolean verifyTableCleared(Connection c, String tempTable, ClearStyle clearStyle) {
        if (selectCount(c, tempTable) != 0) {

            // Try again
            attemptToClearTempTable(c, tempTable, clearStyle);
            if (selectCount(c, tempTable) == 0) {

                String message = "On first attempt, did not clear temp table " + tempTable
                        + " using style " + clearStyle.toString()
                        + " and transaction status: " + transactionAsString(c)
                        + ".  Second attempt was successful.";
                throw new RuntimeException(message);

            } else {

                if (clearStyle.equals(ClearStyle.TRUNCATE)) {
                    attemptToClearTempTable(c, tempTable, ClearStyle.DELETE);
                    if (selectCount(c, tempTable) == 0) {
                        String message = "On multiple attempts with clear style TRUNCATE, did not clear temp table " + tempTable
                                + ", however an attempt with clear style DELETE was successful."
                                + "Transaction status: " + transactionAsString(c) + ".";
                        throw new RuntimeException(message);

                    } else {
                        String message = "On multiple attempts, did not clear temp table " + tempTable
                                + " using both clear styles "
                                + " and transaction status: " + transactionAsString(c) + ".";
                        throw new RuntimeException(message);
                    }
                } else {
                    String message = "On multiple attempts, did not clear temp table " + tempTable
                            + " using style " + clearStyle.toString()
                            + " and transaction status: " + transactionAsString(c) + ".";
                    throw new RuntimeException(message);

                }
            }
        }

        return true;
    }


    void clearTempTableUsingDelete(Connection c, String tempTable) throws PalantirSqlException {
        String sqlDelete = TextUtils.format(SQL_DELETE_PT_TEMP_IDS, tempTable);
        try {
            executeUnregisteredQuery(c, sqlDelete, new Object[]{}); // dynamic query
        } catch (PalantirSqlException e) {
            if (e.getMessage().contains("ORA-08102")) { // "index key not found"
                // Gather some information
                AgnosticResultSet dbaRows = null;
                String objectIdString = null;
                try {
                    Pattern p = Pattern.compile(".*ORA-08102: index key not found, obj# (\\d+),.*");
                    Matcher match = p.matcher(e.getMessage());
                    Validate.isTrue(match.matches(), "Could not find dba_objects object_id in: " + e.getMessage());
                    objectIdString = match.group(1);
                    int objectId = Integer.parseInt(objectIdString);
                    String sql = "SELECT * FROM dba_objects WHERE object_id = ?";
                    dbaRows = selectResultSetUnregisteredQuery(c, sql, objectId);
                } catch (Exception exceptionGettingInfo) {
                    throw new IllegalArgumentException("Problem diagnosing ORA-08102", exceptionGettingInfo);
                }
                throw new RuntimeException(
                        String.format("ORA-08102 encountered, selecting object_id %s from dba_objects: %s",
                                objectIdString, dbaRows),
                        e);
            }
            throw e;
        }
        SqlStats.INSTANCE.incrementClearTempTableByDelete();
    }

    void clearTempTableUsingTruncate(Connection c, String tempTable) throws PalantirSqlException {
        String sqlTruncate = TextUtils.format(SQL_TRUNCATE_PT_TEMP_IDS, tempTable);
        executeUnregisteredQuery(c, sqlTruncate);
        SqlStats.INSTANCE.incrementClearTempTableByTruncate();
    }

    void loadTempIdPairsIntoEight(Connection c, Map<Long, Long> idToField1, ClearStyle clearStyle)
            throws PalantirSqlException {
        List<Object[]> args = pairToEightFields(idToField1);
        loadEightFieldTempIds(c, args, clearStyle);
    }

    void loadIdKeyTuplesTempIds(Connection c, List<Object[]> args) throws PalantirSqlException {
        clearTempTable(c, "PT_TEMP_IDS_ID_KEY_TUPLES", ClearStyle.DELETE); //$NON-NLS-1$
        insertMany(c, SQL_INSERT_INTO_PT_TEMP_IDS_ID_KEY_TUPLES, args);
    }

    void loadTempIdPairsIntoIdKeyTuples(Connection c, Map<Long, Long> idToField1) {
        List<Object[]> args = pairToIdKeyFields(idToField1);
        loadIdKeyTuplesTempIds(c, args);
    }

    private List<Object[]> pairToEightFields(Map<Long, Long> idToField1) {
        List<Object[]> args = Lists.newArrayListWithCapacity(idToField1.size());
        for (Map.Entry<Long, Long> entry : idToField1.entrySet()) {
            Object[] arg = newEmptyIds(9);
            arg[0] = entry.getKey();
            arg[1] = entry.getValue();
            args.add(arg);
        }

        return args;
    }

    private List<Object[]> pairToIdKeyFields(Map<Long, Long> idToField1) {
        List<Object[]> args = Lists.newArrayListWithCapacity(idToField1.size());
        for (Map.Entry<Long, Long> entry : idToField1.entrySet()) {
            Object[] arg = newEmptyIds(10);
            arg[0] = entry.getKey();
            arg[1] = entry.getValue();
            arg[9] = null;
            args.add(arg);
        }

        return args;
    }

    private Object[] newEmptyIds(int size) {
        Object[] arg = new Object[size];
        Arrays.fill(arg, 0L);
        return arg;
    }
}
