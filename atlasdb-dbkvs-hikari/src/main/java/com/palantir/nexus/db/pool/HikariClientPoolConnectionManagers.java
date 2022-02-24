/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.nexus.db.pool;

import com.palantir.nexus.db.pool.config.ConnectionConfig;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HikariClientPoolConnectionManagers {
    private static final ConcurrentMap<ConnectionConfig, HikariCPConnectionManager> SHARED_POOLS =
            new ConcurrentHashMap<>();

    private HikariClientPoolConnectionManagers() {
        // lolz
    }

    public static HikariCPConnectionManager create(ConnectionConfig config) {
        if (config.reuseConnectionPool()) {
            // todo(gmaretic): deal with closing the pool
            return SHARED_POOLS.computeIfAbsent(config, HikariCPConnectionManager::new);
        }
        return new HikariCPConnectionManager(config);
    }
}
