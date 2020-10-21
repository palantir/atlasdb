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
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class AgnosticResultRowImpl extends AbstractAgnosticResultRow {
    private List<Object> columns;

    public AgnosticResultRowImpl(List<Object> cols, DBType type, Map<String, Integer> cmap) {
        super(type, cmap);
        columns = cols;
    }

    @Override
    public Object getObject(int col) {
        return columns.get(col);
    }

    /**
     * Get a value out of a result set, assume it to be a binary field.
     * If null, return empty byte array.  This call is appropriate for H2/Oracle column
     * types of BLOB, or HSQLDB column types of LONGVARBINARY or
     * POSTGRESQL types of BYTEA
     * @param col Zero-based column index
     * @param blobLength the length in bytes of the blob (ignored if negative).
     *    Setting this parameter improves the efficiency of this call by ~30%.
     *    You can obtain this value by retrieving a "length(my_blob)" column in
     *    your ResultSet.
     * @return bytes of the blob
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    @Override
    public byte[] getBlob(int col, int blobLength) throws PalantirSqlException {
        if (col < 0 || col >= columns.size()) {
            throw new IllegalArgumentException(
                    "index: " + col + " is out of bounds for columns: " + columns.size()); // $NON-NLS-1$ //$NON-NLS-2$
        }
        if (columns.get(col) == null) {
            return new byte[0];
        }

        if (dbType == DBType.ORACLE || dbType == DBType.H2_MEMORY) {
            Blob contentBlob = (Blob) columns.get(col);

            if (contentBlob == null || blobLength == 0) {
                return new byte[0];
            }

            if (blobLength < 0) {
                // then we need to ask Oracle what it is
                blobLength = (int) Blobs.length(contentBlob);
            }

            return Blobs.getBytes(contentBlob, 1L, blobLength);
        } else if (dbType == DBType.POSTGRESQL) {
            byte[] blob = (byte[]) columns.get(col);
            if (blob == null) {
                return new byte[0];
            }

            return blob;
        }
        assert false : "unknown db type"; // $NON-NLS-1$
        throw PalantirSqlException.create("unknown db type: " + dbType); // $NON-NLS-1$
    }

    /**
     * @deprecated Use getBlob instead
     */
    @Override
    @Deprecated // Use getBlob instead
    public byte[] getBytes(int col) {
        return (byte[]) columns.get(col);
    }

    @Override
    @Deprecated // Use the get by colname instead
    protected String getClobString(int col, int clobLength) throws PalantirSqlException {
        if (dbType == DBType.ORACLE || dbType == DBType.H2_MEMORY) {
            return columns.get(col) == null ? null : Clobs.getString((Clob) columns.get(col), clobLength);
        } else if (dbType == DBType.POSTGRESQL) {
            return (String) columns.get(col);
        }
        assert false : "unknown db type"; // $NON-NLS-1$
        throw PalantirSqlException.create("unknown db type: " + dbType); // $NON-NLS-1$
    }

    /**
     * Get a value out of a result set, assume it to be a number field.
     * If null, use fallback value supplied (as when user wants -1 for value
     * not present instead of 0).  This call is appropriate for Oracle column
     * types of NUMBER, or H2/HSQLDB / POSTGRESQL column types of BIGINT
     * @deprecated use getLong by colname instead.
     * @param col Zero-based column index
     * @param fallback long value to return if value not present (e.g. null in db)
     * @throws SQLException
     */
    @Override
    @Deprecated // use the get by colname variant instead
    protected long getLong(int col, long fallback) throws PalantirSqlException {
        if (col < 0 || col >= columns.size()) {
            throw new IllegalArgumentException(
                    "index: " + col + " is out of bounds for columns: " + columns.size()); // $NON-NLS-1$ //$NON-NLS-2$
        }
        if (columns.get(col) == null) {
            return fallback;
        } else {
            if (dbType == DBType.ORACLE) {
                return ((BigDecimal) columns.get(col)).longValue();
            } else if (dbType == DBType.POSTGRESQL || dbType == DBType.H2_MEMORY) {
                return ((Number) columns.get(col)).longValue();
            }
            assert false : "unknown db type"; // $NON-NLS-1$
            throw PalantirSqlException.create("unknown db type: " + dbType); // $NON-NLS-1$
        }
    }

    @Override
    @Deprecated // use the get by colname variant instead
    protected Long getLongObject(int col) throws PalantirSqlException {
        if (columns.get(col) == null) {
            return null;
        }
        return Long.valueOf(getLong(col));
    }

    @Override
    protected boolean getBoolean(int col) throws PalantirSqlException {
        Boolean obj = (Boolean) columns.get(col);
        return obj == null ? false : obj.booleanValue();
    }

    @Override
    @Deprecated // use the get by colname variant instead
    protected int getInteger(int col) throws PalantirSqlException {
        if (columns.get(col) == null) {
            return 0;
        }

        if (dbType == DBType.ORACLE) {
            return ((BigDecimal) columns.get(col)).intValue();
        } else if (dbType == DBType.POSTGRESQL || dbType == DBType.H2_MEMORY) {
            Object o = columns.get(col);
            if (o instanceof Integer) {
                return ((Integer) o).intValue();
            } else if (o instanceof Long) {
                return ((Long) o).intValue();
            } else if (o instanceof BigDecimal) {
                return ((BigDecimal) o).intValue();
            }
            String msg = "unknown return type for expected integer column"; // $NON-NLS-1$
            assert false : msg;
            throw PalantirSqlException.create(msg);
        }
        assert false : "no db type defined"; // $NON-NLS-1$
        throw PalantirSqlException.create("unknown db type: " + dbType); // $NON-NLS-1$
    }

    @Override
    @Deprecated // use the get by colname variant instead
    protected double getDouble(int col) throws PalantirSqlException {
        if (columns.get(col) == null) {
            return 0;
        }

        if (dbType == DBType.ORACLE) {
            return ((BigDecimal) columns.get(col)).doubleValue();
        } else if (dbType == DBType.POSTGRESQL || dbType == DBType.H2_MEMORY) {
            return ((Double) columns.get(col)).doubleValue();
        }

        assert false : "unknown db type"; // $NON-NLS-1$
        throw PalantirSqlException.create("unknown db type: " + dbType); // $NON-NLS-1$
    }

    @Override
    @Deprecated // use the get by colname variant instead
    protected Double getDoubleObject(int col) throws PalantirSqlException {
        if (columns.get(col) == null) {
            return null;
        }
        return Double.valueOf(getLong(col));
    }

    @Override
    public int size() {
        return columns.size();
    }

    /**
     * Returns the string value for the column. If the column value is null,
     * return null.
     */
    @Override
    @Deprecated // use the get by colname variant instead
    protected String getNullableString(int col) {
        Object obj = columns.get(col);
        return (obj != null) ? String.valueOf(obj) : null;
    }

    /**
     * Returns the string value for the column. If the column value is null,
     * return empty string.
     */
    @Override
    @Deprecated // use the get by colname variant instead
    protected String getString(int col) {
        Object obj = columns.get(col);
        if (obj == null) {
            return "";
        } // $NON-NLS-1$
        return String.valueOf(obj);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{"); // $NON-NLS-1$
        boolean first = true;
        for (Object column : columns) {
            if (!first) {
                sb.append(", ");
            } // $NON-NLS-1$
            first = false;
            sb.append(column);
        }
        return sb.append("}").toString(); // $NON-NLS-1$
    }
}
