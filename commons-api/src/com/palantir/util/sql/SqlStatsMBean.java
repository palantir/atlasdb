package com.palantir.util.sql;


import javax.management.MXBean;

@MXBean
public interface SqlStatsMBean {
    public boolean isCollectCallStatsEnabled();

    public void setCollectCallStatsEnabled(boolean enabled);

    public void clearAllStats();

    public String getTopQueriesByTotalTime(int n);

    public long getClearTempTableByDeleteCount();
    public long getClearTempTableByTruncateCount();
}
