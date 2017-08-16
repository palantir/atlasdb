/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Optional;

import org.slf4j.Logger;

import com.palantir.async.initializer.UninitializedException;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompaction;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompactionManager;
import com.palantir.processors.AutoDelegate;

@AutoDelegate(typeToExtend = CassandraKeyValueService.class)
public class AsyncInitializedCassandraKeyValueService extends AutoDelegate_CassandraKeyValueService {

    private AsyncInitializedCassandraKeyValueService(Logger log,
            CassandraKeyValueServiceConfigManager configManager,
            Optional<CassandraJmxCompactionManager> compactionManager,
            Optional<LeaderConfig> leaderConfig) {
        super(log, configManager, compactionManager, leaderConfig);
    }

    public static AsyncInitializedCassandraKeyValueService create(
            CassandraKeyValueServiceConfigManager configManager,
            Optional<LeaderConfig> leaderConfig,
            Logger log) {
        Optional<CassandraJmxCompactionManager> compactionManager =
                CassandraJmxCompaction.createJmxCompactionManager(configManager);
        AsyncInitializedCassandraKeyValueService ret = new AsyncInitializedCassandraKeyValueService(
                log,
                configManager,
                compactionManager,
                leaderConfig);
        ret.asyncInitialize();
        return ret;
    }

    @Override
    public CassandraKeyValueService delegate() {
        if (isInitialized()) {
            return this;
        } else {
            throw new UninitializedException("CassandraKeyValueService is not initialized yet.");
        }
    }

}
