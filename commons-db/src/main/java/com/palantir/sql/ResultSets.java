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
package com.palantir.sql;

import com.google.common.collect.Maps;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.BasicSQL;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

public class ResultSets {

    public static Object getObject(ResultSet rs, int col) throws PalantirSqlException {
        try {
            return rs.getObject(col);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static Map<String, Integer> buildInMemoryColumnMap(ResultSetMetaData meta, DBType dbType) {
        return buildColumnMap(meta, 0);
    }

    public static Map<String, Integer> buildJdbcColumnMap(ResultSetMetaData meta, DBType dbType) {
        return buildColumnMap(meta, 1);
    }

    /** @param columnOffset Some result set implementations index their columns from 1, some from 0 */
    private static Map<String, Integer> buildColumnMap(ResultSetMetaData meta, int columnOffset) {
        /* Build a map containing both all lower case versions of the column
         * names. This avoids the CPU and GC cost of case conversion upon lookup
         * compared to using something like newTreeMap(String.CASE_INSENSITIVE_ORDER),
         * but this also requires
         * users of this map to convert keys to lower upper case for access.
         */
        int columnCount = getColumnCount(meta);
        Map<String, Integer> columnMap = Maps.newHashMapWithExpectedSize(2 * columnCount);
        for (int i = 0; i < columnCount; i++) {
            String columnLabel = getColumnLabel(meta, i + 1);
            columnMap.put(columnLabel.toLowerCase(), i + columnOffset);
            columnMap.put(columnLabel.toUpperCase(), i + columnOffset);
        }
        return columnMap;
    }

    public static String getString(ResultSet rs, int col) throws PalantirSqlException {
        try {
            return rs.getString(col);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static long getLong(ResultSet rs, int col) throws PalantirSqlException {
        try {
            return rs.getLong(col);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static long getLong(ResultSet rs, String idColName) {
        try {
            return rs.getLong(idColName);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static double getDouble(ResultSet rs, int col) throws PalantirSqlException {
        try {
            return rs.getDouble(col);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static boolean getBoolean(ResultSet ts, int col) throws PalantirSqlException {
        try {
            return ts.getBoolean(col);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static int getInt(ResultSet rs, int col) throws PalantirSqlException {
        try {
            return rs.getInt(col);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static Timestamp getTimestamp(ResultSet rs, int col) throws PalantirSqlException {
        try {
            return rs.getTimestamp(col);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static ResultSetMetaData getMetaData(ResultSet rs) throws PalantirSqlException {
        try {
            return rs.getMetaData();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static int getColumnCount(ResultSet rs) throws PalantirSqlException {
        try {
            return rs.getMetaData().getColumnCount();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static String getColumnLabel(ResultSet rs, int col) throws PalantirSqlException {
        try {
            return rs.getMetaData().getColumnLabel(col);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static int getColumnCount(ResultSetMetaData metaData) throws PalantirSqlException {
        try {
            return metaData.getColumnCount();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static String getColumnLabel(ResultSetMetaData metaData, int col) throws PalantirSqlException {
        try {
            return metaData.getColumnLabel(col);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static Blob getBlob(ResultSet rs, int col) throws PalantirSqlException {
        try {
            return rs.getBlob(col);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static byte[] getBytes(ResultSet rs, int col) throws PalantirSqlException {
        try {
            return rs.getBytes(col);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static InputStream getBinaryStream(ResultSet rs, int col) throws PalantirSqlException {
        try {
            final InputStream binaryStream = rs.getBinaryStream(col);
            if (binaryStream == null) {
                return null;
            }
            if (binaryStream instanceof ByteArrayInputStream) {
                return binaryStream;
            }

            // Otherwise, make sure the underlying stream is closed only once.
            return new FilterInputStream(binaryStream) {
                boolean closed = false;

                @Override
                public void close() throws IOException {
                    if (!closed) {
                        closed = true;
                        super.close();
                    }
                }
            };
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static Object getArray(ResultSet rs, int col) {
        try {
            return rs.getArray(col).getArray();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static boolean next(ResultSet rs) throws PalantirSqlException {
        try {
            return rs.next();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void close(ResultSet rs) throws PalantirSqlException {
        try {
            rs.close();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }
}
