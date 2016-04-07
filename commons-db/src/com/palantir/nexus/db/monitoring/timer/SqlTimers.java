package com.palantir.nexus.db.monitoring.timer;

import com.google.common.collect.ImmutableList;

public class SqlTimers {
    public static DurationSqlTimer createDurationSqlTimer() {
        return new DurationSqlTimer();
    }

    public static SqlStatsSqlTimer createSqlStatsSqlTimer() {
        return new SqlStatsSqlTimer();
    }

    public static CombinedSqlTimer createCombinedSqlTimer(Iterable<SqlTimer> sqlTimers) {
        return new CombinedSqlTimer(ImmutableList.copyOf(sqlTimers));
    }
}
