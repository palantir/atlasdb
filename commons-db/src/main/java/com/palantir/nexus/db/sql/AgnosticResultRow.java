/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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


import com.palantir.exception.PalantirSqlException;

public interface AgnosticResultRow {

    public boolean containsColumn(String colname);

    public Object getObject(String colname) throws PalantirSqlException;

    public Object getObject(int col) throws PalantirSqlException;

    @Deprecated //use colname instead
    public byte[] getBlob(int col, int blobLength) throws PalantirSqlException;
    public byte[] getBlob(String colname) throws PalantirSqlException;

    public byte[] getBlob(String colname, int blobLength) throws PalantirSqlException;

    @Deprecated // use the getBlob variant instead
    public byte[] getBytes(String colname) throws PalantirSqlException;

    /** Get the value of a result column that is a count, meaning it
     * comes from a count SQL expression like {@code count(*)} or
     * {@code count(id)}. This should be used instead of
     * {@link getLong} or {@link getInteger} because different
     * databases store counts differently. Specifically HSQL is
     * lame.
     * @param colname name of the column for the count. May not be null
     * @throws PalantirSqlException
     */
    public long getCount(String colname) throws PalantirSqlException;

    public String getClobString(String colname) throws PalantirSqlException;

    public String getClobString(String colname, int clobLength) throws PalantirSqlException;

    /**
     * Will return <code>fallback</code> if field is null.
     */
    public long getLong(String colname, long fallback) throws PalantirSqlException;

    /**
     * Will return 0 if field is null.
     */
    public long getLong(String colname) throws PalantirSqlException;

    public Long getLongObject(String colname) throws PalantirSqlException;

    /**
     * Will return false if field is null.
     */
    boolean getBoolean(String colname) throws PalantirSqlException;

    /**
     * Will return 0 if field is null.
     */
    public int getInteger(String colname) throws PalantirSqlException;

    /**
     * Will return 0 if field is null.
     */
    public double getDouble(String colname) throws PalantirSqlException;

    public Double getDoubleObject(String colname) throws PalantirSqlException;

    public String getNullableString(String colname) throws PalantirSqlException;

    /**
     * Will return the empty string if value is null.
     */
    public String getString(String colname) throws PalantirSqlException;

    /**
     * Can we used with {@link #getObject(int)} to get the whole row.
     *
     * @return the number of columns in this row.
     */
    public int size() throws PalantirSqlException;



}
