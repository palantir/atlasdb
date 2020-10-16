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

import com.palantir.common.base.Throwables;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAgnosticResultRow implements AgnosticResultRow {

    private static final Logger log = LoggerFactory.getLogger(AbstractAgnosticResultRow.class);

    protected DBType dbType;
    protected Map<String, Integer> columnMap;

    public AbstractAgnosticResultRow(DBType type, Map<String, Integer> cmap) {
        dbType = type;
        columnMap = cmap;
        assert isValidCaseInsensitiveMap(cmap);
    }

    private static boolean isValidCaseInsensitiveMap(Map<String, ?> cmap) {
        boolean isValid = true;
        for (String colName : cmap.keySet()) {
            isValid &= cmap.containsKey(colName.toLowerCase());
            isValid &= cmap.containsKey(colName.toUpperCase());
            isValid &= (cmap.get(colName.toLowerCase()) == cmap.get(colName.toUpperCase()));
        }
        return isValid;
    }

    protected int findColumn(String colname) {
        // fast path will hit on all lower or all upper case column names
        Integer colIdx = columnMap.get(colname);
        if (colIdx == null) {
            // column name is mixed case or invalid, fall back on slow path
            colIdx = columnMap.get(colname.toLowerCase());
            if (colIdx == null) {
                throw new IllegalArgumentException(
                        "Column named {" + colname + "} not found in result set"); // $NON-NLS-1$ //$NON-NLS-2$
            } else {
                log.debug(
                        "Column name '{}' supplied in mixed case - leads to slower execution of"
                                + " AgnosticResultRow.findColumn",
                        colname); //$NON-NLS-1$
            }
        }
        return colIdx.intValue();
    }

    @Override
    public boolean containsColumn(String colname) {
        // Interestingly, toLowerCase optimizes by returning self if no change is required,
        // and all lower case values will commonly be supplied by the callers,
        // so this is the fastest way to get a false answer
        return (columnMap.containsKey(colname) || columnMap.containsKey(colname.toLowerCase()));
    }

    @Override
    public Object getObject(String colname) throws PalantirSqlException {
        return getObject(findColumn(colname));
    }

    @Override
    public abstract Object getObject(int col) throws PalantirSqlException;

    @Override
    public byte[] getBlob(String colname) throws PalantirSqlException {
        return getBlob(colname, -1);
    }

    @Override
    public byte[] getBlob(String colname, int blobLength) throws PalantirSqlException {
        return getBlob(findColumn(colname), blobLength);
    }

    @Override
    @Deprecated // use the getBlob variant instead
    public byte[] getBytes(String colname) throws PalantirSqlException {
        return getBytes(findColumn(colname));
    }

    @Deprecated // use the getBlob variant instead
    protected abstract byte[] getBytes(int col) throws PalantirSqlException;

    @Override
    public long getCount(String colname) throws PalantirSqlException {
        long count = getLong(colname);
        assert count > -1;
        return count;
    }

    @Override
    @Deprecated
    public abstract byte[] getBlob(int col, int blobLength) throws PalantirSqlException;

    @Override
    public String getClobString(String colname) throws PalantirSqlException {
        return getClobString(findColumn(colname), -1);
    }

    @Override
    public String getClobString(String colname, int clobLength) throws PalantirSqlException {
        return getClobString(findColumn(colname), clobLength);
    }

    @Deprecated // use the get by colname variant instead
    protected abstract String getClobString(int col, int clobLength) throws PalantirSqlException;

    @Override
    public long getLong(String colname, long fallback) throws PalantirSqlException {
        return getLong(findColumn(colname), fallback);
    }

    @Override
    public long getLong(String colname) throws PalantirSqlException {
        return getLong(findColumn(colname));
    }

    /**
     * Get a value out of a result set, assume it to be a number field.
     * If null, use default value of 0).  This call is appropriate for Oracle column
     * types of NUMBER, or HSQLDB column types of BIGINT
     * @param col Zero-based column index
     * @throws PalantirSqlException
     */
    @Deprecated // use the get by colname variant instead
    long getLong(int col) throws PalantirSqlException {
        return getLong(col, 0);
    }

    @Deprecated // use the get by colname variant instead
    protected abstract long getLong(int col, long fallback) throws PalantirSqlException;

    @Override
    public Long getLongObject(String colname) throws PalantirSqlException {
        return getLongObject(findColumn(colname));
    }

    @Deprecated // use the get by colname variant instead
    protected abstract Long getLongObject(int col) throws PalantirSqlException;

    @Override
    public boolean getBoolean(String colname) throws PalantirSqlException {
        return getBoolean(findColumn(colname));
    }

    protected abstract boolean getBoolean(int col) throws PalantirSqlException;

    @Override
    public int getInteger(String colname) throws PalantirSqlException {
        return getInteger(findColumn(colname));
    }

    @Deprecated // use the get by colname variant instead
    protected abstract int getInteger(int col) throws PalantirSqlException;

    @Override
    public double getDouble(String colname) throws PalantirSqlException {
        return getDouble(findColumn(colname));
    }

    @Override
    public Double getDoubleObject(String colname) throws PalantirSqlException {
        return getDoubleObject(findColumn(colname));
    }

    @Deprecated // use the get by colname variant instead
    protected abstract Double getDoubleObject(int col) throws PalantirSqlException;

    @Deprecated // use the get by colname variant instead
    protected abstract double getDouble(int col) throws PalantirSqlException;

    @Override
    public String getNullableString(String colname) throws PalantirSqlException {
        return getNullableString(findColumn(colname));
    }

    @Override
    public String getString(String colname) throws PalantirSqlException {
        return getString(findColumn(colname));
    }

    protected abstract String getNullableString(int col) throws PalantirSqlException;

    protected abstract String getString(int col) throws PalantirSqlException;

    protected String convertReaderToString(final InputStreamReader reader) throws PalantirSqlException {
        if (reader == null) {
            return null;
        }

        try {
            final int SIZE = 1024;
            char[] cbuf = new char[SIZE];

            int length;
            StringBuilder sb = new StringBuilder();
            while ((length = reader.read(cbuf, 0, SIZE)) > 0) {
                sb.append(cbuf, 0, length);
            }

            return sb.toString();
        } catch (IOException e) {
            throw Throwables.chain(
                    PalantirSqlException.createForChaining(),
                    Throwables.chain(
                            new SQLException("Could not convert BufferedReader into a String"), e)); // $NON-NLS-1$
        }
    }

    protected byte[] convertToBytes(final InputStream bias) throws IOException {
        if (bias == null) {
            return new byte[0];
        }

        final int SIZE = 1024;
        byte[] buf = new byte[SIZE];
        int length;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while ((length = bias.read(buf, 0, SIZE)) > 0) {
            baos.write(buf, 0, length);
        }

        return baos.toByteArray();
    }

    @Override
    public abstract int size() throws PalantirSqlException;
}
