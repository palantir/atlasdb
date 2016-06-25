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
        Connection conn = connectionSupplier.get();
        try {
            return helper.execute(conn, sql.getKey(), vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public PreparedStatement execute(String key, Object... vs) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.execute(conn, key, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public void executeUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            helper.executeUnregisteredQuery(conn, sql, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public boolean insertMany(RegisteredSQLString sql, Iterable<Object[]> list) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.insertMany(conn, sql.getKey(), list);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public boolean insertMany(String key, Iterable<Object[]> list) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.insertMany(conn, key, list);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public boolean insertManyUnregisteredQuery(String sql, Iterable<Object[]> list) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.insertManyUnregisteredQuery(conn, sql, list);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public boolean insertOne(RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.insertOne(conn, sql.getKey(), vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public boolean insertOne(String key, Object... vs) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.insertOne(conn, key, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public boolean insertOneUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.insertOneUnregisteredQuery(conn, sql, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public long selectCount(String tableName, String whereClause, Object... vs) throws PalantirSqlException, PalantirInterruptedException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.selectCount(conn, tableName, whereClause, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public long selectCount(String tableName) throws PalantirSqlException, PalantirInterruptedException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.selectCount(conn, tableName);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public boolean selectExists(RegisteredSQLString sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.selectExists(conn, sql.getKey(), vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public boolean selectExists(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.selectExists(conn, key, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public boolean selectExistsUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.selectExistsUnregisteredQuery(conn, sql, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public int selectInteger(RegisteredSQLString sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.selectInteger(conn, sql.getKey(), vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public int selectInteger(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.selectInteger(conn, key, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public int selectIntegerUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.selectIntegerUnregisteredQuery(conn, sql, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public AgnosticLightResultSet selectLightResultSet(RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {

        Connection conn = connectionSupplier.get();
        try {
            return helper.selectLightResultSet(conn, sql.getKey(), vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public AgnosticLightResultSet selectLightResultSet(String key, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {

        Connection conn = connectionSupplier.get();
        try {
            return helper.selectLightResultSet(conn, key, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public AgnosticLightResultSet selectLightResultSetUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {

        Connection conn = connectionSupplier.get();
        try {
            return helper.selectLightResultSetUnregisteredQuery(conn, sql, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public AgnosticResultSet selectResultSet(RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {

        Connection conn = connectionSupplier.get();
        try {
            return helper.selectResultSet(conn, sql.getKey(), vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public AgnosticResultSet selectResultSet(String key, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {

        Connection conn = connectionSupplier.get();
        try {
            return helper.selectResultSet(conn, key, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public AgnosticResultSet selectResultSetUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException {

        Connection conn = connectionSupplier.get();
        try {
            return helper.selectResultSetUnregisteredQuery(conn, sql, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public boolean update(RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.update(conn, sql.getKey(), vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public boolean update(String key, Object... vs) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.update(conn, key, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public int updateCountRows(RegisteredSQLString sql, Object... vs) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.updateCountRows(conn, sql.getKey(), vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public int updateCountRows(String key, Object... vs) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.updateCountRows(conn, key, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public void updateMany(RegisteredSQLString sql, Iterable<Object[]> list) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            helper.updateMany(conn, sql.getKey(), list);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public void updateMany(RegisteredSQLString sql) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            helper.updateMany(conn, sql.getKey());
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public void updateMany(String key, Iterable<Object[]> list) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            helper.updateMany(conn, key, list);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public void updateMany(String key) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            helper.updateMany(conn, key);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public void updateManyUnregisteredQuery(String sql, Iterable<Object[]> list) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            helper.updateManyUnregisteredQuery(conn, sql, list);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public void updateManyUnregisteredQuery(String sql) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            helper.updateManyUnregisteredQuery(conn, sql);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public boolean updateUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException {
        Connection conn = connectionSupplier.get();
        try {
            return helper.updateUnregisteredQuery(conn, sql, vs);
        } finally {
            BasicSQLUtils.close(conn);
        }
    }

    @Override
    public Connection getUnderlyingConnection() {
        return connectionSupplier.get();
    }
}
