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

package com.palantir.atlasdb.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.io.File;
import java.nio.file.Paths;
import org.immutables.value.Value;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = RocksDbPersistentStorageConfig.class, name = RocksDbPersistentStorageConfig.TYPE)})
public interface PersistentStorageConfig {
    String type();

    /**
     * Relative path from the current working directory to the directory in which we want to store our data.
     * In case the directory does not exist we create a new one. Anything in this folder might be deleted.
     *
     * @return relative path to the directory
     */
    String storagePath();

    @Value.Check
    default void check() {
        Preconditions.checkState(
                !Paths.get(storagePath()).isAbsolute(),
                "Storage path must be relative",
                SafeArg.of("storagePath", storagePath()));

        File storageFile = new File(storagePath());
        if (storageFile.exists()) {
            Preconditions.checkState(
                    storageFile.isDirectory(),
                    "Storage path has to point to a directory",
                    SafeArg.of("path", storagePath()));
        }
    }
}
