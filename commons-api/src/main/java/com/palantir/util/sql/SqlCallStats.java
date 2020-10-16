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

    private static final Ordering<SqlCallStats> TOTAL_TIME_ORDERING = Ordering.from((o1, o2) -> {
        int cmp = Longs.compare(o1.getTotalTime(), o2.getTotalTime());
        if (cmp != 0) {
            return cmp;
        }
        return o1.rawSql.compareTo(o2.rawSql);
    });

    public static Ordering<SqlCallStats> getTotalTimeOrderingAscending() {
        return TOTAL_TIME_ORDERING;
    }
}
