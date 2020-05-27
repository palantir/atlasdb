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

import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.sqlite.SQLiteConfig;
import org.sqlite.javax.SQLiteConnectionPoolDataSource;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * This class is responsible for creating Sqlite connections to an instance.
 * There should be one instance per timelock.
 */
public final class SqliteConnections {
    private static final String DEFAULT_SQLITE_DATABASE_NAME = "sqliteData.db";
    private static final LoadingCache<Path, DataSource> DATA_SOURCE_LOADING_CACHE = Caffeine.newBuilder()
            .build(SqliteConnections::buildDataSource);

    private SqliteConnections() {
        // no
    }

    public static DataSource getOrCreateDefaultDataSource(Path path) {
        return DATA_SOURCE_LOADING_CACHE.get(path);
    }

    private static DataSource buildDataSource(Path path) {
        createDirectoryIfNotExists(path);
        String target = String.format("jdbc:sqlite:%s", path.resolve(DEFAULT_SQLITE_DATABASE_NAME).toString());

        SQLiteConfig config = new SQLiteConfig();
        config.setPragma(SQLiteConfig.Pragma.JOURNAL_MODE, SQLiteConfig.JournalMode.WAL.getValue());
        config.setPragma(SQLiteConfig.Pragma.LOCKING_MODE, SQLiteConfig.LockingMode.EXCLUSIVE.getValue());
        config.setPragma(SQLiteConfig.Pragma.SYNCHRONOUS, SQLiteConfig.SynchronousMode.FULL.getValue());

        SQLiteConnectionPoolDataSource dataSource = new SQLiteConnectionPoolDataSource();
        dataSource.setUrl(target);
        dataSource.setConfig(config);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDataSource(dataSource);
        hikariConfig.setMaximumPoolSize(1);
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
