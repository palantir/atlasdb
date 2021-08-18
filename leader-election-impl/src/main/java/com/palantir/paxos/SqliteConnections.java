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

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.sqlite.SQLiteConfig;
import org.sqlite.javax.SQLiteConnectionPoolDataSource;

/**
 * This class is responsible for creating Sqlite connections to an instance.
 * There should be one instance per timelock.
 */
public final class SqliteConnections {
    private static final String DEFAULT_SQLITE_DATABASE_NAME = "sqliteData.db";
    public static final SqliteConnectionConfig DEFAULT_SQLITE_CONNECTION_CONFIG =
            ImmutableSqliteConnectionConfig.builder().build();

    private SqliteConnections() {
        // no
    }

    public static HikariDataSource getPooledDataSource(Path path) {
        return getPooledDataSource(path, DEFAULT_SQLITE_CONNECTION_CONFIG);
    }

    public static HikariDataSource getPooledDataSource(Path path, SqliteConnectionConfig sqliteConnectionConfig) {
        createDirectoryIfNotExists(path);
        String target = String.format(
                "jdbc:sqlite:%s", path.resolve(DEFAULT_SQLITE_DATABASE_NAME).toString());

        SQLiteConfig config = new SQLiteConfig();
        config.setPragma(SQLiteConfig.Pragma.JOURNAL_MODE, SQLiteConfig.JournalMode.WAL.getValue());
        config.setPragma(SQLiteConfig.Pragma.LOCKING_MODE, SQLiteConfig.LockingMode.EXCLUSIVE.getValue());
        config.setPragma(SQLiteConfig.Pragma.SYNCHRONOUS, "EXTRA");

        SQLiteConnectionPoolDataSource dataSource = new SQLiteConnectionPoolDataSource();
        dataSource.setUrl(target);
        dataSource.setConfig(config);

        HikariConfig hikariConfig = sqliteConnectionConfig.getHikariConfig();
        hikariConfig.setDataSource(dataSource);
        return new HikariDataSource(hikariConfig);
    }

    private static void createDirectoryIfNotExists(Path path) {
        try {
            FileUtils.forceMkdir(path.toFile());
        } catch (IOException e) {
            throw new SafeRuntimeException("Could not create directory at path", e, SafeArg.of("path", path));
        }
    }
}
