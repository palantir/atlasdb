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

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.Supplier;

import org.apache.commons.io.FileUtils;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;

/**
 * This class is responsible for creating Sqlite connections to an instance.
 * There should be one instance per timelock.
 */
public final class SqliteConnections {
    private static final String DEFAULT_SQLITE_DATABASE_NAME = "sqliteData.db";

    private SqliteConnections() {
        // no
    }

    public static Supplier<Connection> createDefaultNamedSqliteDatabaseAtPath(Path path) {
        createDirectoryIfNotExists(path);
        String target = String.format("jdbc:sqlite:%s", path.resolve(DEFAULT_SQLITE_DATABASE_NAME).toString());
        return () -> {
            try {
                return DriverManager.getConnection(target);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static void createDirectoryIfNotExists(Path path) {
        try {
            FileUtils.forceMkdir(path.toFile());
        } catch (IOException e) {
            throw new SafeRuntimeException("Could not create directory at path", e, SafeArg.of("path", path));
        }
    }
}
