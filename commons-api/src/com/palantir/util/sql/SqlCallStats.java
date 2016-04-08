package com.palantir.util.sql;


import java.util.Comparator;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.palantir.util.jmx.AbstractOperationStats;


/**
 * MXBean which tracks statistics for a particular SQL query.
 */
public class SqlCallStats extends AbstractOperationStats implements SqlCallStatsMBean {
    private final String name;
    private final String rawSql;

    public SqlCallStats(String name, String rawSql) {
        this.name = name;
        this.rawSql = rawSql;
    }

    @Override
    public String getRawSql() {
        return rawSql;
    }

    @Override
    public String getQueryName() {
        return name;
    }

    public void collectCallTimeNanos(long timeInNs) {
        super.collectOperationTimeNanos(timeInNs);
    }

    @Override
    public double get25thPercentileCallTimeMillis() {
        return getPercentileMillis(25.0);
    }

    @Override
    public double get75thPercentileCallTimeMillis() {
        return getPercentileMillis(75.0);
    }

    private static Ordering<SqlCallStats> TOTAL_TIME_ORDERING = Ordering.from(new Comparator<SqlCallStats>() {

        @Override
        public int compare(SqlCallStats o1, SqlCallStats o2) {
            int cmp = Longs.compare(o1.getTotalTime(), o2.getTotalTime());
            if (cmp != 0) {
                return cmp;
            }
            return o1.rawSql.compareTo(o2.rawSql);
        }
    });

    public static Ordering<SqlCallStats> getTotalTimeOrderingAscending() {
        return TOTAL_TIME_ORDERING;
    }
}
