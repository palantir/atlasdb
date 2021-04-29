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
import com.palantir.nexus.db.DBType;
import com.palantir.sql.Blobs;
import com.palantir.sql.Clobs;
import com.palantir.sql.ResultSets;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Map;
import org.joda.time.DateTime;

public class AgnosticLightResultRowImpl extends AbstractAgnosticResultRow implements AgnosticLightResultRow {
    private ResultSet results;

    public AgnosticLightResultRowImpl(DBType type, Map<String, Integer> columnMap, ResultSet res) {
        super(type, columnMap);
        results = res;
    }

    @Override
    public Object getObject(int col) throws PalantirSqlException {
        return ResultSets.getObject(results, col);
    }

    /**
     * Retrieves the bytes that makes up a blob.
     *
     * @param col index of the column that contains the blob
     * @param blobLength the length in bytes of the blob (ignored if negative).
     *    Setting this parameter improves the efficiency of this call by ~30%.
     *    You can obtain this value by retrieving a "length(my_blob)" column in
     *    your ResultSet.
     * @return bytes of the blob
     * @throws PalantirSqlException
     */
    @Override
    @Deprecated // use the get by colname variant instead
    public byte[] getBlob(int col, int blobLength) throws PalantirSqlException {
        if (dbType == DBType.ORACLE) {
            Blob contentBlob = ResultSets.getBlob(results, col);
            if (contentBlob == null || blobLength == 0) {
                return new byte[0];
            }

            if (blobLength < 0) {
                // then we need to ask Oracle what it is
                blobLength = (int) Blobs.length(contentBlob);
            }

            return Blobs.getBytes(contentBlob, 1L, blobLength);
        } else if (dbType == DBType.POSTGRESQL || dbType == DBType.H2_MEMORY) {
            byte[] blob = ResultSets.getBytes(results, col);
            if (blob == null) {
                return new byte[0];
            }

            return blob;
        }

        assert false : "unsupported db type"; // $NON-NLS-1$
        throw PalantirSqlException.create("unknown db type: " + dbType); // $NON-NLS-1$
    }

    @Override
    @Deprecated // use the getBlob variant instead
    public byte[] getBytes(int col) throws PalantirSqlException {
        return ResultSets.getBytes(results, col);
    }

    @Override
    public InputStream getBinaryInputStream(final String colname) throws PalantirSqlException {
        return getBinaryInputStream(findColumn(colname));
    }

    @Deprecated // use the get by colname variant instead
    InputStream getBinaryInputStream(int col) throws PalantirSqlException {
        return ResultSets.getBinaryStream(results, col);
    }

    @Override
    @Deprecated // use the get by colname variant instead
    protected String getClobString(int col, int clobLength) throws PalantirSqlException {
        if (dbType == DBType.ORACLE) {
            return ResultSets.getObject(results, col) == null
                    ? null
                    : Clobs.getString((Clob) ResultSets.getObject(results, col), clobLength);
        } else if (dbType == DBType.POSTGRESQL || dbType == DBType.H2_MEMORY) {
            return ResultSets.getString(results, col);
        }
        assert false : "unknown db for getClobString"; // $NON-NLS-1$
        throw PalantirSqlException.create("unknown db type: " + dbType); // $NON-NLS-1$
    }

    @Override
    @Deprecated // use the get by colname variant instead
    long getLong(int col) throws PalantirSqlException {
        return ResultSets.getLong(results, col);
    }

    /**
     * This one isn't as fast, probably.  If the Long or BigDecimal
     * doesn't need to be instantiated, it'll probably run faster.
     * @throws PalantirSqlException
     */
    @Override
    @Deprecated // use the get by colname variant instead
    protected long getLong(int col, long fallback) throws PalantirSqlException {
        Object retVal = ResultSets.getObject(results, col);
        if (retVal == null) {
            return fallback;
        } else {
            if (dbType == DBType.ORACLE) {
                return ((BigDecimal) retVal).longValue();
            } else if (dbType == DBType.POSTGRESQL || dbType == DBType.H2_MEMORY) {
                return ResultSets.getLong(results, col);
            } else {
                assert false : "getLong from unknown db type"; // $NON-NLS-1$
                throw PalantirSqlException.create("unknown db type: " + dbType); // $NON-NLS-1$
            }
        }
    }

    @Override
    @Deprecated // use the get by colname variant instead
    protected Long getLongObject(int col) throws PalantirSqlException {
        if (ResultSets.getObject(results, col) == null) {
            return null;
        }
        return getLong(col);
    }

    @Override
    protected boolean getBoolean(int col) throws PalantirSqlException {
        if (ResultSets.getObject(results, col) == null) {
            return false;
        }
        return ResultSets.getBoolean(results, col);
    }

    @Override
    @Deprecated // use the get by colname variant instead
    protected int getInteger(int col) throws PalantirSqlException {
        if (ResultSets.getObject(results, col) == null) {
            return 0;
        }

        if (dbType == DBType.ORACLE) {
            return ((BigDecimal) ResultSets.getObject(results, col)).intValue();
        } else if (dbType == DBType.POSTGRESQL || dbType == DBType.H2_MEMORY) {
            return ResultSets.getInt(results, col);
        } else {
            assert false : "unknown db type for getInteger"; // $NON-NLS-1$
            throw PalantirSqlException.create("unknown db type: " + dbType); // $NON-NLS-1$
        }
    }

    /**
     * Returns the string value for the column. If the column value is null,
     * return null.
     * @throws PalantirSqlException
     */
    @Override
    @Deprecated // use the get by colname variant instead
    protected String getNullableString(int col) throws PalantirSqlException {
        return (String) ResultSets.getObject(results, col);
    }

    @Override
    @Deprecated // use the get by colname variant instead
    protected String getString(int col) throws PalantirSqlException {
        Object obj = ResultSets.getString(results, col);
        if (obj == null) {
            return "";
        } // $NON-NLS-1$
        return String.valueOf(obj);
    }

    @Deprecated // use the get by colname variant instead
    DateTime getDateTime(int col) throws PalantirSqlException {
        Object rawObj = ResultSets.getObject(results, col);
        if (rawObj == null) {
            return null;
        }
        //		return new DateTime(ResultSets.getTimestamp(col), extractDateTimeZone(rawObj));
        Timestamp timestamp = ResultSets.getTimestamp(results, col);
        return new DateTime(timestamp);
    }

    @Override
    public DateTime getDateTime(String colname) throws PalantirSqlException {
        return getDateTime(findColumn(colname));
    }

    @Override
    @Deprecated // use the get by colname variant instead
    protected double getDouble(int col) throws PalantirSqlException {
        return ResultSets.getDouble(results, col);
    }

    @Override
    @Deprecated // use the get by colname variant instead
    protected Double getDoubleObject(int col) throws PalantirSqlException {
        if (ResultSets.getObject(results, col) == null) {
            return null;
        }
        return getDouble(col);
    }

    @Override
    public Object getArray(String colname) throws PalantirSqlException {
        return getArray(findColumn(colname));
    }

    private Object getArray(int col) throws PalantirSqlException {
        return ResultSets.getArray(results, col);
    }

    @Override
    public int size() throws PalantirSqlException {
        return ResultSets.getColumnCount(results);
    }
}
