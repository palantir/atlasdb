/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.rocksdb;

import java.io.File;
import java.util.Map;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.rocksdb.impl.ImmutableWriteOpts;
import com.palantir.atlasdb.keyvalue.rocksdb.impl.WriteOpts;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

@JsonDeserialize(as = ImmutableRocksDbKeyValueServiceConfig.class)
@JsonSerialize(as = ImmutableRocksDbKeyValueServiceConfig.class)
@JsonTypeName(RocksDbKeyValueServiceConfig.TYPE)
@Value.Immutable
public abstract class RocksDbKeyValueServiceConfig implements KeyValueServiceConfig {

    public static final String TYPE = "rocksdb";

    public abstract File dataDir();

    public abstract Map<String, String> dbOptions();
    public abstract Map<String, String> cfOptions();

    @Value.Default
    public WriteOpts writeOptions() {
        return ImmutableWriteOpts.builder().build();
    }

    @Value.Default
    public File nativeLibTmpDir() {
        return new File(System.getProperty("java.io.tmpdir"));
    }

    @Value.Check
    protected final void check() {
        Preconditions.checkArgument(dataDir().exists() || dataDir().mkdirs(),
                "dataDir '%s' does not exist and cannot be created.", dataDir());

        // Doing this here is not really ideal, but we need to do this very
        // early in the process to prevent the default loading of the libraries
        // to java.io.tmpdir that occurs as soon as any rocksdb classes are loaded.
        RocksDbNativeLibraryLoader.load(nativeLibTmpDir());
    }

    @Override
    public final String type() {
        return TYPE;
    }

}
