package com.palantir.util.sql;


import javax.management.MXBean;

@MXBean
public interface SqlCallStatsMBean {
    public String getQueryName();
    public String getRawSql();

    public void clearStats();
    public long getTotalTime();
    public long getTotalCalls();
    public long getTimePerCallInMillis();
    public double getStandardDeviationInMillis();
    public long getMaxCallTime();
    public long getMinCallTime();
    public double getPercentileMillis(double perc);
    public double getPercentCallsFinishedInMillis(int millis);
    public double getMedianTimeRequestInMillis();

    // The following are redundant with getPercentileMillis, but they should
    // make information easier to browse from jconsole.
    public double get25thPercentileCallTimeMillis();
    public double get75thPercentileCallTimeMillis();
}
