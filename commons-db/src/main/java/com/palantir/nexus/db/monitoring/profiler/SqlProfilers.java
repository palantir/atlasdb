package com.palantir.nexus.db.monitoring.profiler;

public class SqlProfilers {
    public static SqlProfiler getSqlProfiler() {
        return SimpleSqlProfiler.INSTANCE;
    }
}
