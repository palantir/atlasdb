package com.palantir.nexus.db.monitoring.timer;

import com.palantir.nexus.db.monitoring.profiler.SqlProfilers;
import com.palantir.util.sql.SqlStats;

final public class DurationSqlTimer implements SqlTimer {
    @Override
    public Handle start(String module, final String sqlKey, final String rawSql) {
        final long startNs = System.nanoTime();
        return new Handle() {
            @Override
            public void stop() {
                long durationNs = System.nanoTime() - startNs;
                SqlStats.INSTANCE.updateStats(sqlKey, rawSql, durationNs);
                SqlProfilers.getSqlProfiler().update(sqlKey, rawSql, durationNs);
            }
        };
    }
}
