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

import com.palantir.exception.PalantirInterruptedException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.SQLString.RegisteredSQLString;
import com.palantir.sql.Connections;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.function.Supplier;

/**
 * Prefer {@link PalantirSqlConnectionImpl} to this class.
 *
 * This should only be used in places where you have no {@link SharedBackendSession}.
 */
public class ConnectionBackedSqlConnectionImpl implements ConnectionBackedSqlConnection {
    private final Connection c;
    private final Supplier<Long> timestampSupplier;
    private final SqlConnectionHelper sqlConnectionHelper;

    public ConnectionBackedSqlConnectionImpl(
            Connection c, Supplier<Long> timestampSupplier, SqlConnectionHelper sqlConnectionHelper) {
        this.c = c;
        this.timestampSupplier = timestampSupplier;
        this.sqlConnectionHelper = sqlConnectionHelper;
    }

    @Override
    public void initializeTempTable(TempTable tempTable) throws PalantirSqlException {
        tempTable.initialize(c);
    }

    @Override
    public PreparedStatement execute(RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.execute(c, sql.getKey(), vs);
    }

    @Override
    public PreparedStatement execute(String key, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.execute(c, key, vs);
    }

    @Override
    public int executeCountRows(RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.executeCountRows(c, sql.getKey(), vs);
    }

    @Override
    public int executeCountRows(String key, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.executeCountRows(c, key, vs);
    }

    @Override
    public void executeUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException {
        sqlConnectionHelper.executeUnregisteredQuery(c, sql, vs);
    }

    @Override
    public int executeUnregisteredQueryCountRows(String sql, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.executeUnregisteredQueryCountRows(c, sql, vs);
    }

    @Override
    public boolean insertMany(RegisteredSQLString sql, Iterable<Object[]> list) throws PalantirSqlException {
        return sqlConnectionHelper.insertMany(c, sql.getKey(), list);
    }

    @Override
    public boolean insertMany(String key, Iterable<Object[]> list) throws PalantirSqlException {
        return sqlConnectionHelper.insertMany(c, key, list);
    }

    @Override
    public boolean insertManyUnregisteredQuery(String sql, Iterable<Object[]> list) throws PalantirSqlException {
        return sqlConnectionHelper.insertManyUnregisteredQuery(c, sql, list);
    }

    @Override
    public boolean insertOne(RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.insertOne(c, sql.getKey(), vs);
    }

    @Override
    public boolean insertOne(String key, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.insertOne(c, key, vs);
    }

    @Override
    public int insertOneCountRows(RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.insertOneCountRows(c, sql.getKey(), vs);
    }

    @Override
    public int insertOneCountRows(String key, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.insertOneCountRows(c, key, vs);
    }

    @Override
    public int insertOneCountRowsUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.insertOneCountRowsUnregisteredQuery(c, sql, vs);
    }

    @Override
    public boolean insertOneUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.insertOneUnregisteredQuery(c, sql, vs);
    }

    @Override
    public long selectCount(String tableName, String whereClause, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectCount(c, tableName, whereClause, vs);
    }

    @Override
    public long selectCount(String tableName) throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectCount(c, tableName);
    }

    @Override
    public boolean selectExists(RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectExists(c, sql.getKey(), vs);
    }

    @Override
    public boolean selectExists(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectExists(c, key, vs);
    }

    @Override
    public boolean selectExistsUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectExistsUnregisteredQuery(c, sql, vs);
    }

    @Override
    public int selectInteger(RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectInteger(c, sql.getKey(), vs);
    }

    @Override
    public int selectInteger(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectInteger(c, key, vs);
    }

    @Override
    public int selectIntegerUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectIntegerUnregisteredQuery(c, sql, vs);
    }

    @Override
    public AgnosticLightResultSet selectLightResultSet(RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectLightResultSet(c, sql.getKey(), vs);
    }

    @Override
    public AgnosticLightResultSet selectLightResultSet(String key, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectLightResultSet(c, key, vs);
    }

    @Override
    public AgnosticLightResultSet selectLightResultSetUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectLightResultSetUnregisteredQuery(c, sql, vs);
    }

    @Override
    public AgnosticLightResultSet selectLightResultSetUnregisteredQueryWithFetchSize(
            String sql, int fetchSize, Object... vs) throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectLightResultSetUnregisteredQueryWithFetchSize(c, sql, fetchSize, vs);
    }

    @Override
    public long selectLong(RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectLong(c, sql.getKey(), vs);
    }

    @Override
    public long selectLong(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectLong(c, key, vs);
    }

    @Override
    public long selectLongUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectLongUnregisteredQuery(c, sql, vs);
    }

    @Override
    public Long selectLongWithDefault(RegisteredSQLString sql, Long defaultVal, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectLongWithDefault(c, sql.getKey(), defaultVal, vs);
    }

    @Override
    public Long selectLongWithDefault(String key, Long defaultVal, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectLongWithDefault(c, key, defaultVal, vs);
    }

    @Override
    public Long selectLongWithDefaultUnregisteredQuery(String sql, Long defaultVal, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectLongWithDefaultUnregisteredQuery(c, sql, defaultVal, vs);
    }

    @Override
    public AgnosticResultSet selectResultSet(RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectResultSet(c, sql.getKey(), vs);
    }

    @Override
    public AgnosticResultSet selectResultSet(String key, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectResultSet(c, key, vs);
    }

    @Override
    public AgnosticResultSet selectResultSetUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectResultSetUnregisteredQuery(c, sql, vs);
    }

    @Override
    public boolean update(RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.update(c, sql.getKey(), vs);
    }

    @Override
    public boolean update(String key, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.update(c, key, vs);
    }

    @Override
    public int updateCountRows(RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.updateCountRows(c, sql.getKey(), vs);
    }

    @Override
    public int updateCountRows(String key, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.updateCountRows(c, key, vs);
    }

    @Override
    public int updateCountRowsUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.updateCountRowsUnregisteredQuery(c, sql, vs);
    }

    @Override
    public void updateMany(RegisteredSQLString sql, Iterable<Object[]> list) throws PalantirSqlException {
        sqlConnectionHelper.updateMany(c, sql.getKey(), list);
    }

    @Override
    public void updateMany(RegisteredSQLString sql) throws PalantirSqlException {
        sqlConnectionHelper.updateMany(c, sql.getKey());
    }

    @Override
    public void updateMany(String key, Iterable<Object[]> list) throws PalantirSqlException {
        sqlConnectionHelper.updateMany(c, key, list);
    }

    @Override
    public void updateMany(String key) throws PalantirSqlException {
        sqlConnectionHelper.updateMany(c, key);
    }

    @Override
    public void updateManyUnregisteredQuery(String sql, Iterable<Object[]> list) throws PalantirSqlException {
        sqlConnectionHelper.updateManyUnregisteredQuery(c, sql, list);
    }

    @Override
    public boolean updateUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException {
        return sqlConnectionHelper.updateUnregisteredQuery(c, sql, vs);
    }

    @Override
    public long getTimestamp() {
        return timestampSupplier.get();
    }

    @Override
    public void close() throws PalantirSqlException {
        Connections.close(c);
    }

    @Override
    public Connection getUnderlyingConnection() {
        return c;
    }
}
