/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.paxos;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.Supplier;

/**
 * This class is responsible for creating Sqlite connections to an instance.
 * There should be one instance per timelock.
 */
public class Sqlites {
    private Sqlites() {
        // no
    }

    public static Supplier<Connection> createDatabaseForTests() {
        return createSqliteDatabase(":memory:");
    }

    public static Supplier<Connection> createSqliteDatabase(String path) {
        String target = String.format("jdbc:sqlite:%s", path);
        return () -> {
            try {
                return DriverManager.getConnection(target);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
