package com.palantir.nexus.db.sql;

import com.google.common.collect.ImmutableList;
import com.palantir.nexus.db.monitoring.timer.SqlTimer;
import com.palantir.nexus.db.monitoring.timer.SqlTimers;
import com.palantir.nexus.db.sql.BasicSQL.SqlConfig;

public abstract class BasicSqlConfig implements SqlConfig {
    protected Iterable<SqlTimer> getSqlTimers() {
        return ImmutableList.of(
                SqlTimers.createDurationSqlTimer(),
                SqlTimers.createSqlStatsSqlTimer());
    }

    @Override
    final public SqlTimer getSqlTimer() {
        return SqlTimers.createCombinedSqlTimer(getSqlTimers());
    }
}
