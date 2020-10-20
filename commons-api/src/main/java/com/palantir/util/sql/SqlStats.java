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
package com.palantir.util.sql;

import com.palantir.util.JMXUtils;
import com.palantir.util.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MXBean which tracks statistics for all SQL queries made by the server.
 */
public final class SqlStats implements SqlStatsMBean {
    public static final SqlStats INSTANCE = createSqlStats();

    private static SqlStats createSqlStats() {
        SqlStats sqlStats = new SqlStats();
        sqlStats.registerWithJMX();
        return sqlStats;
    }

    private SqlStats() {
        //
    }

    private boolean collectCallStatsEnabled = false;

    /** Map which holds all SQL query stats beans. */
    private final Map<String, SqlCallStats> statsByName = new HashMap<String, SqlCallStats>();

    /** Map which tracks query names assigned to unregistered queries. */
    private final Map<String, String> namesByUnregisteredSql = new HashMap<String, String>();

    private final AtomicLong clearTempTableByDeleteCount = new AtomicLong(0);
    private final AtomicLong clearTempTableByTruncateCount = new AtomicLong(0);

    private static final String OBJECT_NAME_STRING = "com.palantir.util.sql:type=DatabaseCalls"; // $NON-NLS-1$

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Don't wish to break the API
    public void registerWithJMX() {
        JMXUtils.registerMBeanCatchAndLogExceptions(this, OBJECT_NAME_STRING);
    }

    @Override
    public synchronized boolean isCollectCallStatsEnabled() {
        return collectCallStatsEnabled;
    }

    /**
     * Enables/disables collection of statistics for all SQL calls. SQL call
     * statistic collection is meant for profiling purposes only, and should not
     * be enabled all the time.
     */
    @Override
    public synchronized void setCollectCallStatsEnabled(boolean enabled) {
        this.collectCallStatsEnabled = enabled;
    }

    /**
     * Updates tracked statistics after a SQL query has executed.
     *
     * @param sqlName the name of the query (the query's registered key), or
     *        null if the query is unregistered (a name will be automatically
     *        assigned in this case).
     * @param rawSql the raw SQL for the query.
     * @param timeInNs the total amount of time in nanoseconds spent executing
     *        the query.
     */
    @SuppressWarnings("ParameterAssignment") // I don't want to copy the query key for perf reasons
    public synchronized void updateStats(String sqlName, String rawSql, long timeInNs) {
        if (!collectCallStatsEnabled) {
            return;
        }

        if (sqlName == null) {
            sqlName = getNameForUnregisteredQuery(rawSql);
        }

        SqlCallStats stats = statsByName.get(sqlName);
        if (stats == null) {
            stats = new SqlCallStats(sqlName, rawSql);
            JMXUtils.registerMBeanCatchAndLogExceptions(stats, getStatsBeanName(stats));
            statsByName.put(sqlName, stats);
        }

        stats.collectCallTimeNanos(timeInNs);
    }

    /**
     * Clears/resets all tracked statistics.
     */
    @Override
    public synchronized void clearAllStats() {
        for (SqlCallStats stats : statsByName.values()) {
            JMXUtils.unregisterMBeanCatchAndLogExceptions(getStatsBeanName(stats));
        }
        namesByUnregisteredSql.clear();
        statsByName.clear();
    }

    /**
     * Returns a textual summary of the top numQueries queries ordered by total time.
     */
    @Override
    public synchronized String getTopQueriesByTotalTime(int numQueries) {
        List<Pair<Long, SqlCallStats>> entries = new ArrayList<Pair<Long, SqlCallStats>>();
        for (SqlCallStats stats : statsByName.values()) {
            entries.add(new Pair<Long, SqlCallStats>(stats.getTotalTime(), stats));
        }

        entries.sort(Pair.compareLhSide());
        Collections.reverse(entries);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(entries.size(), numQueries); ++i) {
            long totalTimeMs = entries.get(i).lhSide;
            SqlCallStats stats = entries.get(i).rhSide;
            String line = String.format(
                    "total_time_ms=%-15d total_calls=%-10d query=%s%n", //$NON-NLS-1$
                    totalTimeMs, stats.getTotalCalls(), stats.getQueryName());
            sb.append(line);
        }
        return sb.toString();
    }

    private String getNameForUnregisteredQuery(String rawSql) {
        String name = namesByUnregisteredSql.computeIfAbsent(
                rawSql,
                k -> String.format(
                        "UNREGISTERED_QUERY_%03d", //$NON-NLS-1$
                        namesByUnregisteredSql.size() + 1));
        return name;
    }

    private String getStatsBeanName(SqlCallStats bean) {
        return OBJECT_NAME_STRING + ",name=" + bean.getQueryName(); // $NON-NLS-1$
    }

    @Override
    public long getClearTempTableByDeleteCount() {
        return clearTempTableByDeleteCount.get();
    }

    @Override
    public long getClearTempTableByTruncateCount() {
        return clearTempTableByTruncateCount.get();
    }

    public void incrementClearTempTableByDelete() {
        clearTempTableByDeleteCount.incrementAndGet();
    }

    public void incrementClearTempTableByTruncate() {
        clearTempTableByTruncateCount.incrementAndGet();
    }
}
