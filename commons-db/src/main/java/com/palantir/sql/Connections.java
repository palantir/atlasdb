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
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;


public class Connections  {

    public static String getUrl(Connection c) throws PalantirSqlException {
        try {
            return c.getMetaData().getURL();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static PreparedStatement prepareStatement(Connection c, String sql)
            throws PalantirSqlException {
        try {
            return c.prepareStatement(sql);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static Statement createStatement(Connection c)
            throws PalantirSqlException {
        try {
            return c.createStatement();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static DatabaseMetaData getMetaData(Connection c)
            throws PalantirSqlException {
        try {
            return c.getMetaData();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static String getUserName(Connection c)
            throws PalantirSqlException {
        try {
            return c.getMetaData().getUserName();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void close(Connection c) throws PalantirSqlException {
        try {
            c.close();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void rollback(Connection c) throws PalantirSqlException {
        try {
            c.rollback();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void setAutoCommit(Connection c, boolean condition) throws PalantirSqlException {
        try {
            c.setAutoCommit(condition);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static boolean getAutoCommit(Connection c) throws PalantirSqlException {
        try {
            return c.getAutoCommit();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void setTransactionIsolation(Connection c, int level) throws PalantirSqlException {
        try {
            c.setTransactionIsolation(level);
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static int getTransactionIsolation(Connection c) throws PalantirSqlException {
        try {
            return c.getTransactionIsolation();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static void commit(Connection c) throws PalantirSqlException {
        try {
            c.commit();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }
}
