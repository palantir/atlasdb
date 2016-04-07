package com.palantir.nexus.db.monitoring.profiler;

import java.util.Collection;

import com.palantir.nexus.db.sql.BasicSQL;
import com.palantir.util.sql.SqlCallStats;

/**
 * Code extracted from {@link BasicSQL}. This provides one of several competing and overlapping
 * mechanisms to register monitoring callbacks for various SQL actions.
 *
 * @author jweel
 */
public interface SqlProfiler {
    void start();

    void update(String sqlKey, String rawSql, long durationNs);

    Collection<SqlCallStats> stop();

    void addSqlProfilerListener(SqlProfilerListener sqlProfilerListener);

    void removeSqlProfilerListener(SqlProfilerListener sqlProfilerListener);

    public interface SqlProfilerListener {
        void traceEvent(String key, long durationNs);
    }
}