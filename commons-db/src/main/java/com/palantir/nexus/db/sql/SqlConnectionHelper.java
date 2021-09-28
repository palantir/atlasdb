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
package com.palantir.nexus.db.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.exception.PalantirInterruptedException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.BasicSQL.AutoClose;
import com.palantir.nexus.db.sql.BasicSQLString.FinalSQLString;
import com.palantir.nexus.db.sql.SQLString.RegisteredSQLString;
import com.palantir.sql.PreparedStatements;
import java.sql.Connection;
import java.sql.PreparedStatement;
import javax.annotation.Nullable;

/**
 * Helper methods for {@link PalantirSqlConnection} implementations. These were originally
 * deprecated static methods on {@link SQL}.
 *
 * @author jweel
 */
@SuppressWarnings("BadAssert") // performance sensitive asserts
public final class SqlConnectionHelper {

    private final BasicSQL basicSql;

    public SqlConnectionHelper(BasicSQL basicSql) {
        this.basicSql = basicSql;
    }

    void executeUnregisteredQuery(Connection c, String sql, Object... vs) throws PalantirSqlException {
        basicSql.execute(c, SQLString.getUnregisteredQuery(sql), vs, AutoClose.TRUE);
    }

    int executeUnregisteredQueryCountRows(Connection c, String sql, Object... vs) throws PalantirSqlException {
        return executeCountRows(c, SQLString.getUnregisteredQuery(sql), vs);
    }

    PreparedStatement execute(Connection c, String key, Object... vs) throws PalantirSqlException {
        return basicSql.execute(c, SQLString.getByKey(key, c), vs, AutoClose.TRUE);
    }

    public int executeCountRows(Connection c, String key, Object... vs) throws PalantirSqlException {
        return executeCountRows(c, SQLString.getByKey(key, c), vs);
    }

    private int executeCountRows(Connection c, FinalSQLString sql, Object... vs) throws PalantirSqlException {
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
     * @param tableName
     * @return
     * @throws PalantirSqlException
     */
    long selectCount(Connection c, String tableName) throws PalantirSqlException, PalantirInterruptedException {
        return selectCount(c, tableName, null, new Object[] {});
    }

    long selectCount(Connection c, String tableName, String whereClause, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        String sql = "SELECT count(*) from " + tableName; // $NON-NLS-1$
        if (whereClause != null) {
            sql += " WHERE " + whereClause; // $NON-NLS-1$
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

    boolean selectExists(Connection c, String key, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
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

    int selectInteger(Connection c, String key, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
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
    long selectLong(Connection c, String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException {
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
    Long selectLongWithDefaultUnregisteredQuery(Connection c, String sql, Long defaultVal, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return basicSql.selectLongInternal(c, SQLString.getUnregisteredQuery(sql), vs, defaultVal, false);
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

    AgnosticLightResultSet selectLightResultSetUnregisteredQuery(Connection c, String sql, Object... vs) {
        return selectLightResultSetUnregisteredQueryWithFetchSize(c, sql, null, vs);
    }

    AgnosticLightResultSet selectLightResultSetUnregisteredQueryWithFetchSize(
            Connection c, String sql, @Nullable Integer fetchSize, Object... vs) {
        return basicSql.selectLightResultSetSpecifyingDBType(
                c, SQLString.getUnregisteredQuery(sql), vs, DBType.getTypeFromConnection(c), fetchSize);
    }

    AgnosticLightResultSet selectLightResultSet(Connection c, String key, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return selectLightResultSet(c, SQLString.getByKey(key, c), vs);
    }

    AgnosticLightResultSet selectLightResultSet(Connection c, FinalSQLString finalSql, Object... vs)
            throws PalantirSqlException {
        return basicSql.selectLightResultSetSpecifyingDBType(c, finalSql, vs, DBType.getTypeFromConnection(c), null);
    }

    AgnosticLightResultSet selectLightResultSet(Connection c, RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        DBType dbType = DBType.getTypeFromConnection(c);
        return basicSql.selectLightResultSetSpecifyingDBType(
                c, SQLString.getByKey(sql.getKey(), dbType), vs, dbType, null);
    }

    AgnosticResultSet selectResultSetUnregisteredQuery(Connection c, String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return basicSql.selectResultSetSpecifyingDBType(
                c, SQLString.getUnregisteredQuery(sql), vs, DBType.getTypeFromConnection(c));
    }

    AgnosticResultSet selectResultSet(Connection c, String key, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        DBType dbType = DBType.getTypeFromConnection(c);
        return basicSql.selectResultSetSpecifyingDBType(c, SQLString.getByKey(key, dbType), vs, dbType);
    }

    AgnosticResultSet selectResultSet(Connection c, RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return selectResultSet(c, sql.getKey(), vs);
    }

    boolean updateUnregisteredQuery(Connection c, String sql, Object... vs) throws PalantirSqlException {
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

    int updateCountRowsUnregisteredQuery(Connection c, String sql, Object... vs) throws PalantirSqlException {
        return basicSql.updateCountRowsInternal(c, SQLString.getUnregisteredQuery(sql), vs);
    }

    int updateCountRows(Connection c, String key, Object... vs) throws PalantirSqlException {
        return basicSql.updateCountRowsInternal(c, SQLString.getByKey(key, c), vs);
    }

    int updateCountRows(Connection c, RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        return updateCountRows(c, sql.getKey(), vs);
    }

    void updateManyUnregisteredQuery(Connection c, String sql, Iterable<Object[]> list) throws PalantirSqlException {
        basicSql.updateMany(c, SQLString.getUnregisteredQuery(sql), Iterables.toArray(list, Object[].class));
    }

    void updateMany(Connection c, String key) throws PalantirSqlException {
        updateMany(c, key, ImmutableList.<Object[]>of());
    }

    void updateMany(Connection c, String key, Iterable<Object[]> list) throws PalantirSqlException {
        basicSql.updateMany(c, SQLString.getByKey(key, c), Iterables.toArray(list, Object[].class));
    }

    void updateMany(Connection c, RegisteredSQLString sql) throws PalantirSqlException {
        updateMany(c, sql.getKey(), ImmutableList.<Object[]>of());
    }

    void updateMany(Connection c, RegisteredSQLString sql, Iterable<Object[]> list) throws PalantirSqlException {
        updateMany(c, sql.getKey(), list);
    }

    boolean insertOneUnregisteredQuery(Connection c, String sql, Object... vs) throws PalantirSqlException {
        final int updated = insertOneCountRowsUnregisteredQuery(c, sql, vs);
        assert updated == 1 : "expected 1 update, got : " + updated; // $NON-NLS-1$
        return true;
    }

    boolean insertOne(Connection c, String key, Object... vs) throws PalantirSqlException {
        final int updated = insertOneCountRows(c, key, vs);
        assert updated == 1 : "expected 1 update, got : " + updated; // $NON-NLS-1$
        return true;
    }

    boolean insertOne(Connection c, RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        return insertOne(c, sql.getKey(), vs);
    }

    int insertOneCountRowsUnregisteredQuery(Connection c, String sql, Object... vs) throws PalantirSqlException {
        return basicSql.insertOneCountRowsInternal(c, SQLString.getUnregisteredQuery(sql), vs);
    }

    int insertOneCountRows(Connection c, String key, Object... vs) throws PalantirSqlException {
        return basicSql.insertOneCountRowsInternal(c, SQLString.getByKey(key, c), vs);
    }

    int insertOneCountRows(Connection c, RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        return insertOneCountRows(c, sql.getKey(), vs);
    }

    boolean insertManyUnregisteredQuery(Connection c, String sql, Iterable<Object[]> list) throws PalantirSqlException {
        return basicSql.insertMany(c, SQLString.getUnregisteredQuery(sql), Iterables.toArray(list, Object[].class));
    }

    boolean insertMany(Connection c, String key, Iterable<Object[]> list) throws PalantirSqlException {
        return basicSql.insertMany(c, SQLString.getByKey(key, c), Iterables.toArray(list, Object[].class));
    }

    boolean insertMany(Connection c, RegisteredSQLString sql, Iterable<Object[]> list) throws PalantirSqlException {
        return insertMany(c, sql.getKey(), list);
    }
}
