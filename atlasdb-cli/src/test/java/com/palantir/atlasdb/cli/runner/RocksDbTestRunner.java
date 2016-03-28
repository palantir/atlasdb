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
package com.palantir.atlasdb.cli.runner;

import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.cli.command.SingleBackendCommand;
import com.palantir.atlasdb.keyvalue.api.KeyValueServiceConfig;
import com.palantir.atlasdb.rocksdb.RocksDbKeyValueServiceConfig;
import com.palantir.common.base.Throwables;

public class RocksDbTestRunner extends AbstractTestRunner {

    public static final String SIMPLE_ROCKSDB_CONFIG_FILENAME = "simple_rocksdb_config.yml";

    public RocksDbTestRunner(Class<? extends SingleBackendCommand> cmdClass, String... args) {
        super(cmdClass, args);
    }

    @Override
    protected String getKvsConfigFileName() {
        return SIMPLE_ROCKSDB_CONFIG_FILENAME;
    }

    @Override
    protected void cleanup(KeyValueServiceConfig kvsConfig) {
        Preconditions.checkArgument(kvsConfig instanceof RocksDbKeyValueServiceConfig,
                "RocksDbAtlasDbFactory expects a configuration of type RocksDbKeyValueServiceConfig, found %s", kvsConfig.getClass());
        RocksDbKeyValueServiceConfig rocksDbConfig = (RocksDbKeyValueServiceConfig) kvsConfig;
        try {
            FileUtils.deleteDirectory(rocksDbConfig.dataDir());
        } catch (IOException e) {
            Throwables.rewrapAndThrowUncheckedException("Could not cleanup after cli test", e);
        }
    }

}
