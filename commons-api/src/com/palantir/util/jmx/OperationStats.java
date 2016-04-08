package com.palantir.util.jmx;

import com.palantir.annotations.PgNotExtendableApi;
import com.palantir.annotations.PgPublicApi;

@PgPublicApi
@PgNotExtendableApi
public interface OperationStats {

    /**
     * This returns the percentage of calls that were returned in {@code millis} milliseconds
     *
     * These are often referred to as understats u2000 is the percentage of calls finished in 2ms
     * {@code percentCallsFinishedInMillis(2000)} is also known as u2000
     *
     *
     * @param millis
     * @return
     */
    public abstract double getPercentCallsFinishedInMillis(int millis);

    /**
     * This returns the approximate number of milliseconds for a given percentile
     *
     * {@code getPercentileMillis(90)} is also known as tp90
     * {@code getPercentileMillis(50)} is the median
     */
    public abstract double getPercentileMillis(double perc);

    public abstract void clearStats();

    public abstract long getTotalTime();

    public abstract long getTotalCalls();

    public abstract long getTimePerCallInMillis();

    public abstract double getStandardDeviationInMillis();

    public abstract long getMaxCallTime();

    public abstract long getMinCallTime();

    public abstract double getMedianTimeRequestInMillis();

}
