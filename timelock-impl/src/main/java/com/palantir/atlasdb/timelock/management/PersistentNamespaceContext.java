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

package com.palantir.atlasdb.timelock.management;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.Optional;

import javax.sql.DataSource;

import org.immutables.value.Value;

import com.palantir.atlasdb.spi.KeyValueServiceConfig;

@Value.Immutable
public interface PersistentNamespaceContext {
    Path fileDataDirectory();

    DataSource sqliteDataSource();

    boolean isUsingDatabasePersistence();

    static PersistentNamespaceContext of(
            Path fileDataDirectory,
            DataSource sqliteDataSource,
            boolean isUsingDatabasePersistence) {
        return ImmutablePersistentNamespaceContext.builder()
                .fileDataDirectory(fileDataDirectory)
                .sqliteDataSource(sqliteDataSource)
                .isUsingDatabasePersistence(isUsingDatabasePersistence)
                .build();
    }
}
