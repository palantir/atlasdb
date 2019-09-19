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

import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.BasicSQL;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class PreparedStatements {
    public static int getUpdateCount(PreparedStatement ps) throws PalantirSqlException {
        try {
            return ps.getUpdateCount();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static boolean execute(PreparedStatement ps) throws PalantirSqlException {
        try {
            return ps.execute();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static ResultSet executeQuery(PreparedStatement ps) throws PalantirSqlException {
        try {
            return ps.executeQuery();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }


    public static void setObject(PreparedStatement ps, int index, Object obj) throws PalantirSqlException {
        try {
            ps.setObject(index, obj);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void setBytes(PreparedStatement ps, int index, byte[] bytes) throws PalantirSqlException {
        try {
            ps.setBytes(index, bytes);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void setBinaryStream(PreparedStatement ps, int index, InputStream stream, int length) throws PalantirSqlException {
        try {
            ps.setBinaryStream(index, stream, length);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void setInt(PreparedStatement ps, int index, Integer obj) throws PalantirSqlException {
        try {
            ps.setInt(index, obj);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void setLong(PreparedStatement ps, int index, Long obj) throws PalantirSqlException {
        try {
            ps.setLong(index, obj);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void setDouble(PreparedStatement ps, int index, Double obj) throws PalantirSqlException {
        try {
            ps.setDouble(index, obj);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void setFloat(PreparedStatement ps, int index, Float obj) throws PalantirSqlException {
        try {
            ps.setFloat(index, obj);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void setShort(PreparedStatement ps, int index, Short obj) throws PalantirSqlException {
        try {
            ps.setShort(index, obj);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void setByte(PreparedStatement ps, int index, Byte obj) throws PalantirSqlException {
        try {
            ps.setByte(index, obj);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void setTimestamp(PreparedStatement ps, int index, Timestamp obj) throws PalantirSqlException {
        try {
            ps.setTimestamp(index, obj);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void cancel(PreparedStatement ps) throws PalantirSqlException {
        try {
            ps.cancel();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void setMaxRows(PreparedStatement ps, int max) throws PalantirSqlException {
        try {
            ps.setMaxRows(max);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void setFetchSize(PreparedStatement ps, int fetchSize) throws PalantirSqlException {
        try {
            ps.setFetchSize(fetchSize);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void setNull(PreparedStatement ps, int index, int sqlType,
            String typeName) throws PalantirSqlException {
        try {
            ps.setNull(index, sqlType, typeName);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }
}
