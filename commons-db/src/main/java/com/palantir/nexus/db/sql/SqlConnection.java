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

import static com.palantir.nexus.db.sql.SQLString.RegisteredSQLString;

import java.sql.Connection;
import java.sql.PreparedStatement;

import com.palantir.exception.PalantirInterruptedException;
import com.palantir.exception.PalantirSqlException;

public interface SqlConnection {
    public void executeUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException;
    public PreparedStatement execute(String key, Object... vs) throws PalantirSqlException;
    public PreparedStatement execute(RegisteredSQLString sql, Object... vs) throws PalantirSqlException;
    public boolean insertManyUnregisteredQuery(String sql, Iterable<Object[]> list) throws PalantirSqlException;
    public long selectCount(String tableName) throws PalantirSqlException, PalantirInterruptedException;
    public long selectCount(String tableName, String whereClause, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public boolean selectExistsUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public boolean selectExists(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public boolean selectExists(final RegisteredSQLString sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public int selectIntegerUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public int selectInteger(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public int selectInteger(final RegisteredSQLString sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    public AgnosticLightResultSet selectLightResultSetUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public AgnosticLightResultSet selectLightResultSet(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public AgnosticLightResultSet selectLightResultSet(RegisteredSQLString sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public AgnosticResultSet selectResultSetUnregisteredQuery(String sql, Object...vs) throws PalantirSqlException, PalantirInterruptedException;
    public AgnosticResultSet selectResultSet(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public AgnosticResultSet selectResultSet(RegisteredSQLString sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;
    public boolean updateUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException;
    public boolean update(String key, Object... vs) throws PalantirSqlException;
    public boolean update(final RegisteredSQLString sql, Object... vs) throws PalantirSqlException;
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
    public boolean insertMany(String key, Iterable<Object[]> list) throws PalantirSqlException;
    public boolean insertMany(RegisteredSQLString sql, Iterable<Object[]> list) throws PalantirSqlException;

    public Connection getUnderlyingConnection() throws PalantirSqlException;
}

