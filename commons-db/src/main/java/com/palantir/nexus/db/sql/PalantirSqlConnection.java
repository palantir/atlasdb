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

public interface PalantirSqlConnection extends SqlConnection {
    int executeUnregisteredQueryCountRows(String sql, Object... vs) throws PalantirSqlException;

    int executeCountRows(String key, Object... vs) throws PalantirSqlException;

    int executeCountRows(RegisteredSQLString sql, Object... vs) throws PalantirSqlException;

    int updateCountRowsUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException;

    int insertOneCountRowsUnregisteredQuery(String sql, Object... vs) throws PalantirSqlException;

    int insertOneCountRows(String key, Object... vs) throws PalantirSqlException;

    int insertOneCountRows(RegisteredSQLString sql, Object... vs) throws PalantirSqlException;

    /**
     * Returns the long value of the first field selected given a query.
     * <p>
     * If the value of the first field is null then 0 will be returned in prod,
     * but an assert will be thrown in test.
     *
     * @deprecated Use selectLongWithDefaultUnregisteredQuery to control behavior when no or NULL results are found.
     */
    @Override
    @Deprecated
    long selectLongUnregisteredQuery(String sql, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

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
    Long selectLongWithDefaultUnregisteredQuery(String sql, Long defaultVal, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    /**
     * Returns the long value of the first field selected given a query If
     * no rows are returned, will return defaultVal.
     * <p>
     * If the value of the first field is null, then defaultVal will be returned. This means
     * that if defaultVal is non-null, then this method won't return null.
     */
    Long selectLongWithDefault(String key, Long defaultVal, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    /**
     * Returns the long value of the first field selected given a query If
     * no rows are returned, will return defaultVal.
     * <p>
     * If the value of the first field is null, then defaultVal will be returned. This means
     * that if defaultVal is non-null, then this method won't return null.
     */
    Long selectLongWithDefault(RegisteredSQLString sql, Long defaultVal, Object... vs)
            throws PalantirSqlException, PalantirInterruptedException;

    void initializeTempTable(TempTable tempTable) throws PalantirSqlException;

    /**
     * This will return the current time.  This is better than {@link System#currentTimeMillis()} because it will take
     * drift into account if this host has a different time than the database host.  Using this method everywhere will
     * ensure that all wall clock time will be drawn from a consistent clock (the DB node).
     *
     * @return the current wall clock time.
     */
    long getTimestamp();
}
