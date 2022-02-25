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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public final class HikariClientPoolConnectionManagers {
    private HikariClientPoolConnectionManagers() {
        // lolz
    }

    public static HikariCPConnectionManager create(
            ConnectionConfig config, Optional<AtomicReference<Object>> connectionManagerRegistrar) {
        if (connectionManagerRegistrar.isPresent()) {
            AtomicReference<Object> registrar = connectionManagerRegistrar.get();
            if (registrar.get() == null) {
                synchronized (registrar) {
                    if (registrar.get() == null) {
                        registrar.set(new HikariCPConnectionManager(config));
                    }
                }
            }
            return (HikariCPConnectionManager) registrar.get();
        }
        return new HikariCPConnectionManager(config);
    }
}
