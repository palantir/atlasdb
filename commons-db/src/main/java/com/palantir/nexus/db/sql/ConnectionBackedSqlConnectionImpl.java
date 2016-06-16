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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.base.Supplier;
import com.palantir.exception.PalantirInterruptedException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.SQLString.RegisteredSQLString;
import com.palantir.sql.Connections;

/**
 * Prefer {@link PalantirSqlConnectionImpl} to this class.
 *
 * This should only be used in places where you have no {@link SharedBackendSession}.
 */
public class ConnectionBackedSqlConnectionImpl implements ConnectionBackedSqlConnection {
    private final Connection c;
    private final Supplier<Long> timestampSupplier;
    private final SqlConnectionHelper sqlConnectionHelper;


    public ConnectionBackedSqlConnectionImpl(Connection c,
                                             Supplier<Long> timestampSupplier,
                                             SqlConnectionHelper sqlConnectionHelper) {
        this.c = c;
        this.timestampSupplier = timestampSupplier;
        this.sqlConnectionHelper = sqlConnectionHelper;
    }

    @Override
    public void clearTempTable(String tempTable) throws PalantirSqlException {
        sqlConnectionHelper.clearTempTable(c, tempTable, ClearStyle.DELETE);
    }

    @Override
    public void deleteIdsFromTempIds(Collection<Long> ids) throws PalantirSqlException {
        sqlConnectionHelper.deleteIdsFromTempIds(c, ids);
    }

    @Override
    public void deleteIdsFromTempIdsForTable(Collection<Long> ids, String tempTable)
        throws PalantirSqlException {
        sqlConnectionHelper.deleteIdsFromTempIdsForTable(c, ids, tempTable);
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
    public void executeUnregisteredQuery(String sql, Object... vs)
        throws PalantirSqlException {
        sqlConnectionHelper.executeUnregisteredQuery(c, sql, vs);
    }

    @Override
    public int executeUnregisteredQueryCountRows(String sql, Object... vs)
        throws PalantirSqlException {
        return sqlConnectionHelper.executeUnregisteredQueryCountRows(c, sql, vs);
    }

    @Override
    public boolean insertMany(RegisteredSQLString sql, Iterable<Object[]> list)
        throws PalantirSqlException {
        return sqlConnectionHelper.insertMany(c, sql.getKey(), list);
    }

    @Override
    public boolean insertMany(String key, Iterable<Object[]> list)
        throws PalantirSqlException {
        return sqlConnectionHelper.insertMany(c, key, list);
    }

    @Override
    public boolean insertManyUnregisteredQuery(String sql, Iterable<Object[]> list)
        throws PalantirSqlException {
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
    public int insertOneCountRowsUnregisteredQuery(String sql, Object... vs)
        throws PalantirSqlException {
        return sqlConnectionHelper.insertOneCountRowsUnregisteredQuery(c, sql, vs);
    }

    @Override
    public boolean insertOneUnregisteredQuery(String sql, Object... vs)
        throws PalantirSqlException {
        return sqlConnectionHelper.insertOneUnregisteredQuery(c, sql, vs);
    }

    @Override
    public void loadTempIdPairsIntoEight(Map<Long, Long> idToField1) throws PalantirSqlException {
        sqlConnectionHelper.loadTempIdPairsIntoEight(c, idToField1, ClearStyle.DELETE);
    }

    @Override
    public void loadEightFieldTempIds(Iterable<Object[]> args) throws PalantirSqlException {
        sqlConnectionHelper.loadEightFieldTempIds(c, args, ClearStyle.DELETE);
    }

    @Override
    public void loadTempIds(Iterable<Long> tempIds, String tableName)
        throws PalantirSqlException {
        sqlConnectionHelper.loadTempIds(c, tempIds, tableName, ClearStyle.DELETE);
    }

    @Override
    public void loadTempIds(Iterable<Long> tempIds) throws PalantirSqlException {
        sqlConnectionHelper.loadTempIds(c, tempIds, ClearStyle.DELETE);
    }

    @Override
    public void loadIdKeyPairTempIds(Iterable<Object[]> args) throws PalantirSqlException {
        sqlConnectionHelper.loadIdKeyPairTempIds(c, args, ClearStyle.DELETE);
    }

    @Override
    public void loadTempExternalIds(Iterable<Object[]> externalIds) throws PalantirSqlException {
        sqlConnectionHelper.loadTempExternalIds(c, externalIds, ClearStyle.DELETE);
    }

    @Override
    public void loadThreeFieldTempIds(Iterable<Object[]> tempIds) throws PalantirSqlException {
        sqlConnectionHelper.loadThreeFieldTempIds(c, tempIds, ClearStyle.DELETE);
    }

    @Override
    public void loadKeysTempIds(Iterable<String> args) throws PalantirSqlException {
        sqlConnectionHelper.loadKeysTempIds(c, args, ClearStyle.DELETE);
    }

    @Override
    public long selectCount(String tableName, String whereClause, Object... vs)
        throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectCount(c, tableName, whereClause, vs);
    }

    @Override
    public long selectCount(String tableName) throws PalantirSqlException,
        PalantirInterruptedException {
        return sqlConnectionHelper.selectCount(c, tableName);
    }

    @Override
    public boolean selectExists(RegisteredSQLString sql, Object... vs) throws PalantirSqlException,
        PalantirInterruptedException {
        return sqlConnectionHelper.selectExists(c, sql.getKey(), vs);
    }

    @Override
    public boolean selectExists(String key, Object... vs) throws PalantirSqlException,
        PalantirInterruptedException {
        return sqlConnectionHelper.selectExists(c, key, vs);
    }

    @Override
    public boolean selectExistsUnregisteredQuery(String sql, Object... vs)
        throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectExistsUnregisteredQuery(c, sql, vs);
    }

    @Override
    public int selectInteger(RegisteredSQLString sql, Object... vs) throws PalantirSqlException,
        PalantirInterruptedException {
        return sqlConnectionHelper.selectInteger(c, sql.getKey(), vs);
    }

    @Override
    public int selectInteger(String key, Object... vs) throws PalantirSqlException,
        PalantirInterruptedException {
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
    public AgnosticLightResultSet selectLightResultSetUnregisteredQuery(String sql,
        Object... vs) throws PalantirSqlException, PalantirInterruptedException {
        return sqlConnectionHelper.selectLightResultSetUnregisteredQuery(c, sql, vs);
    }

    @Override
    public long selectLong(RegisteredSQLString sql, Object... vs) throws PalantirSqlException,
        PalantirInterruptedException {
        return sqlConnectionHelper.selectLong(c, sql.getKey(), vs);
    }

    @Override
    public long selectLong(String key, Object... vs) throws PalantirSqlException,
        PalantirInterruptedException {
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
    public Long selectLongWithDefaultUnregisteredQuery(String sql, Long defaultVal,
        Object... vs) throws PalantirSqlException, PalantirInterruptedException {
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
    public AgnosticResultSet selectResultSetUnregisteredQuery(String sql,
        Object... vs) throws PalantirSqlException, PalantirInterruptedException {
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
    public int updateCountRowsUnregisteredQuery(String sql, Object... vs)
        throws PalantirSqlException {
        return sqlConnectionHelper.updateCountRowsUnregisteredQuery(c, sql, vs);
    }

    @Override
    public void updateMany(RegisteredSQLString sql, Iterable<Object[]> list)
        throws PalantirSqlException {
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
    public void updateManyUnregisteredQuery(String sql, Iterable<Object[]> list)
        throws PalantirSqlException {
        sqlConnectionHelper.updateManyUnregisteredQuery(c, sql, list);
    }

    @Override
    public void updateManyUnregisteredQuery(String sql) throws PalantirSqlException {
        sqlConnectionHelper.updateManyUnregisteredQuery(c, sql);
    }

    @Override
    public boolean updateUnregisteredQuery(String sql, Object... vs)
        throws PalantirSqlException {
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

    @Override
    public void loadIdKeyTuplesTempIds(List<Object[]> tempRows)
            throws PalantirSqlException {
        sqlConnectionHelper.loadIdKeyTuplesTempIds(c, tempRows);
    }

    @Override
    public void loadTempIdPairsIntoIdKeyTuples(Map<Long, Long> idToField1)
            throws PalantirSqlException {
        sqlConnectionHelper.loadTempIdPairsIntoIdKeyTuples(c, idToField1);
    }
}
