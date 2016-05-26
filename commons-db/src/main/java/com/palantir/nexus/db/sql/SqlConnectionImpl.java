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

import com.google.common.base.Supplier;
import com.palantir.exception.PalantirInterruptedException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.SQLString.RegisteredSQLString;

public class SqlConnectionImpl implements SqlConnection {
    protected final SqlConnectionHelper helper;
    protected final Supplier<Connection> connectionSupplier;

    public SqlConnectionImpl(Supplier<Connection> connectionSupplier, SqlConnectionHelper helper) {
        this.helper = helper;
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    public PreparedStatement execute(RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        return helper.execute(connectionSupplier.get(), sql.getKey(), vs);
    }

    @Override
    public PreparedStatement execute(String key, Object... vs) throws PalantirSqlException {
        return helper.execute(connectionSupplier.get(), key, vs);
    }

    @Override
    public void executeUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException {
        helper.executeUnregisteredQuery(connectionSupplier.get(), sql, vs);
    }

    @Override
    public boolean insertMany(RegisteredSQLString sql, Iterable<Object[]> list)
            throws PalantirSqlException {
        return helper.insertMany(connectionSupplier.get(), sql.getKey(), list);
    }

    @Override
    public boolean insertMany(String key, Iterable<Object[]> list)
            throws PalantirSqlException {
        return helper.insertMany(connectionSupplier.get(), key, list);
    }

    @Override
    public boolean insertManyUnregisteredQuery(String sql, Iterable<Object[]> list)
            throws PalantirSqlException {
        return helper.insertManyUnregisteredQuery(connectionSupplier.get(), sql, list);
    }

    @Override
    public boolean insertOne(RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        return helper.insertOne(connectionSupplier.get(), sql.getKey(), vs);
    }

    @Override
    public boolean insertOne(String key, Object... vs) throws PalantirSqlException {
        return helper.insertOne(connectionSupplier.get(), key, vs);
    }

    @Override
    public boolean insertOneUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException {
        return helper.insertOneUnregisteredQuery(connectionSupplier.get(), sql, vs);
    }

    @Override
    public long selectCount(String tableName, String whereClause, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return helper.selectCount(connectionSupplier.get(), tableName, whereClause, vs);
    }

    @Override
    public long selectCount(String tableName) throws PalantirSqlException,
            PalantirInterruptedException {
        return helper.selectCount(connectionSupplier.get(), tableName);
    }

    @Override
    public boolean selectExists(RegisteredSQLString sql, Object... vs) throws PalantirSqlException,
            PalantirInterruptedException {
        return helper.selectExists(connectionSupplier.get(), sql.getKey(), vs);
    }

    @Override
    public boolean selectExists(String key, Object... vs) throws PalantirSqlException,
            PalantirInterruptedException {
        return helper.selectExists(connectionSupplier.get(), key, vs);
    }

    @Override
    public boolean selectExistsUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return helper.selectExistsUnregisteredQuery(connectionSupplier.get(), sql, vs);
    }

    @Override
    public int selectInteger(RegisteredSQLString sql, Object... vs) throws PalantirSqlException,
            PalantirInterruptedException {
        return helper.selectInteger(connectionSupplier.get(), sql.getKey(), vs);
    }

    @Override
    public int selectInteger(String key, Object... vs) throws PalantirSqlException,
            PalantirInterruptedException {
        return helper.selectInteger(connectionSupplier.get(), key, vs);
    }

    @Override
    public int selectIntegerUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return helper.selectIntegerUnregisteredQuery(connectionSupplier.get(), sql, vs);
    }

    @Override
    public AgnosticLightResultSet selectLightResultSet(RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return helper.selectLightResultSet(connectionSupplier.get(), sql.getKey(), vs);
    }

    @Override
    public AgnosticLightResultSet selectLightResultSet(String key, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return helper.selectLightResultSet(connectionSupplier.get(), key, vs);
    }

    @Override
    public AgnosticLightResultSet selectLightResultSetUnregisteredQuery(String sql,
                                                                        Object... vs) throws PalantirSqlException, PalantirInterruptedException {
        return helper.selectLightResultSetUnregisteredQuery(connectionSupplier.get(), sql, vs);
    }

    @Override
    public AgnosticResultSet selectResultSet(RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return helper.selectResultSet(connectionSupplier.get(), sql.getKey(), vs);
    }

    @Override
    public AgnosticResultSet selectResultSet(String key, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        return helper.selectResultSet(connectionSupplier.get(), key, vs);
    }

    @Override
    public AgnosticResultSet selectResultSetUnregisteredQuery(String sql,
                                                              Object... vs) throws PalantirSqlException, PalantirInterruptedException {
        return helper.selectResultSetUnregisteredQuery(connectionSupplier.get(), sql, vs);
    }

    @Override
    public boolean update(RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        return helper.update(connectionSupplier.get(), sql.getKey(), vs);
    }

    @Override
    public boolean update(String key, Object... vs) throws PalantirSqlException {
        return helper.update(connectionSupplier.get(), key, vs);
    }

    @Override
    public int updateCountRows(RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        return helper.updateCountRows(connectionSupplier.get(), sql.getKey(), vs);
    }

    @Override
    public int updateCountRows(String key, Object... vs) throws PalantirSqlException {
        return helper.updateCountRows(connectionSupplier.get(), key, vs);
    }

    @Override
    public void updateMany(RegisteredSQLString sql, Iterable<Object[]> list)
            throws PalantirSqlException {
        helper.updateMany(connectionSupplier.get(), sql.getKey(), list);
    }

    @Override
    public void updateMany(RegisteredSQLString sql) throws PalantirSqlException {
        helper.updateMany(connectionSupplier.get(), sql.getKey());
    }

    @Override
    public void updateMany(String key, Iterable<Object[]> list) throws PalantirSqlException {
        helper.updateMany(connectionSupplier.get(), key, list);
    }

    @Override
    public void updateMany(String key) throws PalantirSqlException {
        helper.updateMany(connectionSupplier.get(), key);
    }

    @Override
    public void updateManyUnregisteredQuery(String sql, Iterable<Object[]> list)
            throws PalantirSqlException {
        helper.updateManyUnregisteredQuery(connectionSupplier.get(), sql, list);
    }

    @Override
    public void updateManyUnregisteredQuery(String sql) throws PalantirSqlException {
        helper.updateManyUnregisteredQuery(connectionSupplier.get(), sql);
    }

    @Override
    public boolean updateUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException {
        return helper.updateUnregisteredQuery(connectionSupplier.get(), sql, vs);
    }

    @Override
    public Connection getUnderlyingConnection() {
        return connectionSupplier.get();
    }
}
