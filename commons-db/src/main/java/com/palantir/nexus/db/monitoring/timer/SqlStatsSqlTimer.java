/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.nexus.db.monitoring.timer;

import com.palantir.nexus.db.sql.monitoring.logger.SqlLoggers;
import com.palantir.util.jmx.OperationTimer;
import com.palantir.util.jmx.OperationTimer.TimingState;
import com.palantir.util.timer.LoggingOperationTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SqlStatsSqlTimer implements SqlTimer {
    private static final Logger logger = SqlLoggers.LOGGER;
    private final OperationTimer timer = LoggingOperationTimer.create(logger);

    @Override
    public Handle start(String module, final String sqlKey, final String rawSql) {
        final TimingState timingState;
        if (LoggerFactory.getLogger("com.palantir.nexus.db.SQL").isTraceEnabled()) {
            String sql = rawSql.replaceAll("[\n]+", "");
            String msg = System.currentTimeMillis() + "\tSQL\t" + module + "\t" + sql;
            timingState = timer.begin(msg);
        } else {
            timingState = null;
        }
        return () -> {
            if (timingState != null && logger.isTraceEnabled()) {
                timingState.end();
            }
        };
    }
}
