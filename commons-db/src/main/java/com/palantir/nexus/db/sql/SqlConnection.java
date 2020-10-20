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
import java.sql.Connection;
import java.sql.PreparedStatement;

public interface SqlConnection {
    void executeUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException;

    PreparedStatement execute(String key, Object... vs) throws PalantirSqlException;

    PreparedStatement execute(RegisteredSQLString sql, Object... vs) throws PalantirSqlException;

    boolean insertManyUnregisteredQuery(String sql, Iterable<Object[]> list) throws PalantirSqlException;

    long selectCount(String tableName) throws PalantirSqlException, PalantirInterruptedException;

    long selectCount(String tableName, String whereClause, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    boolean selectExistsUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    boolean selectExists(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    boolean selectExists(RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    int selectIntegerUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    long selectLongUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    int selectInteger(String key, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    int selectInteger(RegisteredSQLString sql, Object... vs) throws PalantirSqlException, PalantirInterruptedException;

    AgnosticLightResultSet selectLightResultSetUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    AgnosticLightResultSet selectLightResultSetUnregisteredQueryWithFetchSize(String sql, int fetchSize, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    AgnosticLightResultSet selectLightResultSet(String key, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    AgnosticLightResultSet selectLightResultSet(RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    AgnosticResultSet selectResultSetUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    AgnosticResultSet selectResultSet(String key, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    AgnosticResultSet selectResultSet(RegisteredSQLString sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    boolean updateUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException;

    boolean update(String key, Object... vs) throws PalantirSqlException;

    boolean update(RegisteredSQLString sql, Object... vs) throws PalantirSqlException;

    int updateCountRows(String key, Object... vs) throws PalantirSqlException;

    int updateCountRows(RegisteredSQLString sql, Object... vs) throws PalantirSqlException;

    void updateManyUnregisteredQuery(String sql, Iterable<Object[]> list) throws PalantirSqlException;

    void updateMany(String key) throws PalantirSqlException;

    void updateMany(String key, Iterable<Object[]> list) throws PalantirSqlException;

    void updateMany(RegisteredSQLString sql) throws PalantirSqlException;

    void updateMany(RegisteredSQLString sql, Iterable<Object[]> list) throws PalantirSqlException;

    boolean insertOneUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException;

    boolean insertOne(String key, Object... vs) throws PalantirSqlException;

    boolean insertOne(RegisteredSQLString sql, Object... vs) throws PalantirSqlException;

    boolean insertMany(String key, Iterable<Object[]> list) throws PalantirSqlException;

    boolean insertMany(RegisteredSQLString sql, Iterable<Object[]> list) throws PalantirSqlException;

    Connection getUnderlyingConnection() throws PalantirSqlException;
}
