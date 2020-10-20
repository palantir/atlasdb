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
package com.palantir.nexus.db.monitoring.profiler;

import com.google.common.collect.ImmutableList;
import com.palantir.common.concurrent.ExecutorInheritableThreadLocal;
import com.palantir.util.AssertUtils;
import com.palantir.util.sql.SqlCallStats;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum SimpleSqlProfiler implements SqlProfiler {
    INSTANCE;

    private static final Logger log = LoggerFactory.getLogger(SimpleSqlProfiler.class);

    private final ExecutorInheritableThreadLocal<ConcurrentMap<String, SqlCallStats>> currentTrace =
            new ExecutorInheritableThreadLocal<ConcurrentMap<String, SqlCallStats>>();

    /**
     * Presumably, the rather obscure {@link CopyOnWriteArrayList} collection was chosen here to
     * ensure that profilers can be registered and removed in a thread-safe way and that they are
     * always executed in the same order, without requiring that {@link SqlProfilerListener}
     * implement {@link Comparable}, and at the expense of making {@line
     * #removeSqlProfilerListener(SqlProfilerListener)} O(N). (jweel)
     */
    private final Collection<SqlProfilerListener> sqlProfilerListeners = new CopyOnWriteArrayList<>();

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
            AssertUtils.assertAndLog(log, false, "Tracing already started.");
            return;
        }
        currentTrace.set(new ConcurrentHashMap<String, SqlCallStats>());
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
            AssertUtils.assertAndLog(log, false, "Tracing already stopped.");
            return ImmutableList.of();
        }
        Collection<SqlCallStats> result =
                ImmutableList.copyOf(currentTrace.get().values());
        currentTrace.remove();
        return result;
    }
}
