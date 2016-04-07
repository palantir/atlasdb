package com.palantir.nexus.db.monitoring.timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.nexus.db.sql.monitoring.logger.SqlLoggers;
import com.palantir.util.jmx.OperationTimer;
import com.palantir.util.jmx.OperationTimer.TimingState;
import com.palantir.util.timer.LoggingOperationTimer;

final class SqlStatsSqlTimer implements SqlTimer {
    private final Logger logger = SqlLoggers.LOGGER;
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
        return new Handle() {
            @Override
            public void stop() {
                if (timingState != null && logger.isTraceEnabled()) {
                    timingState.end();
                }
            }
        };
    }
}