/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.off.heap.rocksdb;

import java.io.File;
import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.local.storage.spi.LocalStorageConfig;
import com.palantir.logsafe.Preconditions;

@JsonDeserialize(as = ImmutableRocksDBLocalStorageConfig.class)
@JsonSerialize(as = ImmutableRocksDBLocalStorageConfig.class)
@JsonTypeName(RocksDBLocalStorageConfig.TYPE)
@Value.Immutable
public interface RocksDBLocalStorageConfig extends LocalStorageConfig {
    String TYPE = "rocksdb";

    @Override
    default String type() {
        return TYPE;
    }

    Optional<File> location();

    @Value.Check
    default void check() {
        location().ifPresent(
                file -> Preconditions.checkState(file.isDirectory(), "RocksDB config requires empty directory"));
    }
}
