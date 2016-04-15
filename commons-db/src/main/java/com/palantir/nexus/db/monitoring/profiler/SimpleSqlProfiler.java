package com.palantir.nexus.db.monitoring.profiler;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.common.concurrent.ExecutorInheritableThreadLocal;
import com.palantir.util.AssertUtils;
import com.palantir.util.sql.SqlCallStats;

enum SimpleSqlProfiler implements SqlProfiler {
    INSTANCE;

    private final ExecutorInheritableThreadLocal<ConcurrentMap<String, SqlCallStats>> currentTrace = new ExecutorInheritableThreadLocal<ConcurrentMap<String, SqlCallStats>>();

    /**
     * Presumably, the rather obscure {@link CopyOnWriteArrayList} collection was chosen here to
     * ensure that profilers can be registered and removed in a thread-safe way and that they are
     * always executed in the same order, without requiring that {@link SqlProfilerListener}
     * implement {@link Comparable}, and at the expense of making {@line
     * #removeSqlProfilerListener(SqlProfilerListener)} O(N). (jweel)
     */
    private final Collection<SqlProfilerListener> sqlProfilerListeners = Lists.newCopyOnWriteArrayList();

    @Override
    public void addSqlProfilerListener(SqlProfilerListener sqlProfilerListener) {
        sqlProfilerListeners.add(sqlProfilerListener);
    }

    @Override
    public void removeSqlProfilerListener(SqlProfilerListener sqlProfilerListener) {
        sqlProfilerListeners.remove(sqlProfilerListener);
    }

    @Override
    public void start() {
        if (currentTrace.get() != null) {
            AssertUtils.assertAndLog(false, "Tracing already started.");
            return;
        }
        currentTrace.set(Maps.<String, SqlCallStats> newConcurrentMap());
    }

    @Override
    public void update(@Nullable String sqlKey, String rawSql, long durationNs) {
        for (SqlProfilerListener sqlProfilerListener : sqlProfilerListeners) {
            sqlProfilerListener.traceEvent(sqlKey != null ? sqlKey : rawSql, durationNs);
        }

        ConcurrentMap<String, SqlCallStats> m = currentTrace.get();
        if (m == null) {
            // Tracing is disabled.
            return;
        }
        if (!m.containsKey(rawSql)) {
            m.putIfAbsent(rawSql, new SqlCallStats(sqlKey, rawSql));
        }
        SqlCallStats s = m.get(rawSql);
        s.collectCallTimeNanos(durationNs);
    }

    @Override
    public Collection<SqlCallStats> stop() {
        if (currentTrace.get() == null) {
            AssertUtils.assertAndLog(false, "Tracing already stopped.");
            return ImmutableList.of();
        }
        Collection<SqlCallStats> result = ImmutableList.copyOf(currentTrace.get().values());
        currentTrace.remove();
        return result;
    }
}
