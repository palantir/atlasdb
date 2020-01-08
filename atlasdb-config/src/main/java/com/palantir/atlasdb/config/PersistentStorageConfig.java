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

import java.io.File;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(
                value = ImmutableRocksDbPersistentStorageConfig.class,
                name = RocksDbPersistentStorageConfig.TYPE)})
public interface PersistentStorageConfig {
    String type();

    /**
     * Path to the directory in which we want to store the data. In case the directory does not exist we create a new
     * one.
     *
     * @return path to the directory
     */
    String storagePath();

    @Value.Check
    default void check() {
        File storageFile = new File(storagePath());
        if (storageFile.exists()) {
            Preconditions.checkState(
                    storageFile.isDirectory(),
                    "Storage path has to point to a directory",
                    SafeArg.of("path", storagePath()));
        }
    }
}
