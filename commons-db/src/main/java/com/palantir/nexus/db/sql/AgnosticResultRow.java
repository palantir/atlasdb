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

import com.palantir.exception.PalantirSqlException;

public interface AgnosticResultRow {

    boolean containsColumn(String colname);

    Object getObject(String colname) throws PalantirSqlException;

    Object getObject(int col) throws PalantirSqlException;

    @Deprecated // use colname instead
    byte[] getBlob(int col, int blobLength) throws PalantirSqlException;

    byte[] getBlob(String colname) throws PalantirSqlException;

    byte[] getBlob(String colname, int blobLength) throws PalantirSqlException;

    @Deprecated // use the getBlob variant instead
    byte[] getBytes(String colname) throws PalantirSqlException;

    /** Get the value of a result column that is a count, meaning it
     * comes from a count SQL expression like {@code count(*)} or
     * {@code count(id)}. This should be used instead of
     * {@link getLong} or {@link getInteger} because different
     * databases store counts differently. Specifically HSQL is
     * lame.
     * @param colname name of the column for the count. May not be null
     * @throws PalantirSqlException
     */
    long getCount(String colname) throws PalantirSqlException;

    String getClobString(String colname) throws PalantirSqlException;

    String getClobString(String colname, int clobLength) throws PalantirSqlException;

    /**
     * Will return <code>fallback</code> if field is null.
     */
    long getLong(String colname, long fallback) throws PalantirSqlException;

    /**
     * Will return 0 if field is null.
     */
    long getLong(String colname) throws PalantirSqlException;

    Long getLongObject(String colname) throws PalantirSqlException;

    /**
     * Will return false if field is null.
     */
    boolean getBoolean(String colname) throws PalantirSqlException;

    /**
     * Will return 0 if field is null.
     */
    int getInteger(String colname) throws PalantirSqlException;

    /**
     * Will return 0 if field is null.
     */
    double getDouble(String colname) throws PalantirSqlException;

    Double getDoubleObject(String colname) throws PalantirSqlException;

    String getNullableString(String colname) throws PalantirSqlException;

    /**
     * Will return the empty string if value is null.
     */
    String getString(String colname) throws PalantirSqlException;

    /**
     * Can we used with {@link #getObject(int)} to get the whole row.
     *
     * @return the number of columns in this row.
     */
    int size() throws PalantirSqlException;
}
