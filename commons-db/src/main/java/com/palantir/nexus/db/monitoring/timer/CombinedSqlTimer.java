package com.palantir.nexus.db.monitoring.timer;

import java.util.List;

import com.google.common.collect.Lists;

final class CombinedSqlTimer implements SqlTimer {
    private final List<SqlTimer> sqlTimers;

    public CombinedSqlTimer(List<SqlTimer> sqlTimers) {
        this.sqlTimers = sqlTimers;
    }

    @Override
    final public Handle start(String module, String sqlKey, String rawSql) {
        final List<Handle> handles = Lists.newArrayListWithCapacity(sqlTimers.size());
        for (SqlTimer sqlTimer : sqlTimers) {
            handles.add(sqlTimer.start(module, sqlKey, rawSql));
        }
        return new Handle() {
            @Override
            public void stop() {
                for (Handle handle : handles) {
                    handle.stop();
                }
            }
        };
    }
}
