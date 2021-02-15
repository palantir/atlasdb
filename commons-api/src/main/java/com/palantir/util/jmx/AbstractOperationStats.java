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
package com.palantir.util.jmx;

import com.palantir.annotations.PgNotExtendableApi;
import com.palantir.annotations.PgPublicApi;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MXBean;

/*
 * TODO (carrino) consider abstracting this a little bit further so that this
 * isn't time-specific (e.g., this class could be useful for tracking statistics
 * about size of data).
 */

/**
 * Base class for {@link MXBean}s which need to track statistics about how long
 * an operation takes.
 */
@PgPublicApi
@PgNotExtendableApi
public abstract class AbstractOperationStats implements OperationStats {
    private static final long ONE_MILLION = 1000000;

    volatile double doubleSumTimeNanos = 0;
    volatile double doubleSumSquaredTimeNanos = 0;
    volatile long operationTimeNanos = 0;
    volatile long totalCalls = 0;
    volatile long maxCall = 0;
    volatile long minCall = Long.MAX_VALUE;
    final List<AtomicLong> underStatsMillis = new CopyOnWriteArrayList<AtomicLong>();

    @SuppressWarnings("cast")
    protected synchronized void collectOperationTimeNanos(long timeInNanos) {
        operationTimeNanos += timeInNanos;
        totalCalls++;
        double doubleTimeInNanos = (double) timeInNanos;
        doubleSumTimeNanos += doubleTimeInNanos;
        doubleSumSquaredTimeNanos += doubleTimeInNanos * doubleTimeInNanos;
        if (timeInNanos > maxCall) {
            maxCall = timeInNanos;
        }
        if (timeInNanos < minCall) {
            minCall = timeInNanos;
        }
        populateStatsBuckets(timeInNanos);
    }

    protected synchronized void collectOperationTimeMillis(long timeInMillis) {
        collectOperationTimeNanos(timeInMillis * ONE_MILLION);
    }

    private void populateStatsBuckets(long timeInNanos) {
        long timeInMillis = (timeInNanos + ONE_MILLION - 1) / ONE_MILLION;
        int index = 0;
        long upperInclusive = 1;
        while (upperInclusive < timeInMillis) {
            resizeUnderStats(index);
            underStatsMillis.get(index).incrementAndGet();
            index++;
            upperInclusive <<= 1;
        }
        resizeUnderStats(index);
        underStatsMillis.get(index).incrementAndGet();
    }

    private void resizeUnderStats(int index) {
        int diff = index + 1 - underStatsMillis.size();
        for (int i = 0; i < diff; i++) {
            underStatsMillis.add(new AtomicLong());
        }
    }

    @Override
    public double getPercentCallsFinishedInMillis(int millis) {
        if (millis <= 0) {
            return 0.0;
        }
        long localTotalCalls = totalCalls;
        if (localTotalCalls == 0) {
            return 100.0;
        }

        long upper = 1;
        int comparisonPoint = 1; // comparisonPoint = 1 corresponds to % of numbers > 1
        while (upper < millis) {
            upper <<= 1;
            comparisonPoint++;
        }
        resizeUnderStats(comparisonPoint);
        double prev = 1.0 - ((double) underStatsMillis.get(comparisonPoint - 1).get()) / localTotalCalls;
        double next = 1.0 - ((double) underStatsMillis.get(comparisonPoint).get()) / localTotalCalls;
        // lerp(x0, y0, x1, y1, x) -> y
        return 100.0 * lerp(upper / 2.0, prev, upper, next, millis);
    }

    @Override
    @SuppressWarnings("cast")
    public double getPercentileMillis(double perc) {
        double maxInMillis = (double) maxCall / ONE_MILLION;
        if (perc >= 100.0) {
            return maxInMillis;
        }
        if (perc <= 0.0) {
            return 0;
        }
        long localTotalCalls = totalCalls;
        if (localTotalCalls == 0) {
            return 0.0;
        }

        double mustBeBellow = 100.0 - perc;

        long millis = 1;
        int comparisonPoint = 1; // comparisonPoint = 1 corresponds to bucket of things > 1
        resizeUnderStats(comparisonPoint);

        double prevPerc = 100.0;
        double percent = 100.0 * ((double) underStatsMillis.get(comparisonPoint).get()) / localTotalCalls;
        while (percent > mustBeBellow) {
            comparisonPoint++;
            millis <<= 1;
            resizeUnderStats(comparisonPoint);
            prevPerc = percent;
            percent = 100.0 * ((double) underStatsMillis.get(comparisonPoint).get()) / localTotalCalls;
        }
        double ret = lerp(prevPerc, millis / 2.0, percent, millis, mustBeBellow);
        if (ret > maxInMillis) {
            return maxInMillis;
        }
        return ret;
    }

    @SuppressWarnings("checkstyle:ParameterName") // x as a parameter name makes sense here as a Cartesian coordinate
    private double lerp(double x0, double y0, double x1, double y1, double x) {
        return y0 + (x - x0) * (y1 - y0) / (x1 - x0);
    }

    @Override
    public synchronized void clearStats() {
        doubleSumTimeNanos = 0;
        doubleSumSquaredTimeNanos = 0;
        operationTimeNanos = 0;
        totalCalls = 0;
        maxCall = 0;
        minCall = Long.MAX_VALUE;
        for (AtomicLong al : underStatsMillis) {
            al.set(0);
        }
    }

    @Override
    public long getTotalTime() {
        return operationTimeNanos / ONE_MILLION;
    }

    @Override
    public long getTotalCalls() {
        return totalCalls;
    }

    @Override
    public long getTimePerCallInMillis() {
        long denominator = totalCalls;
        if (denominator <= 0) {
            return 0;
        }
        return getTotalTime() / denominator;
    }

    @Override
    public double getStandardDeviationInMillis() {
        Double count = (double) totalCalls;
        if (count == 0) {
            return 0;
        }
        double mean = doubleSumTimeNanos / count;
        double nanosVariance = (doubleSumSquaredTimeNanos / count) - (mean * mean);
        if (nanosVariance < 0) {
            nanosVariance *= -1;
        }
        return Math.sqrt(nanosVariance) * 1e-6;
    }

    @Override
    public long getMaxCallTime() {
        return maxCall / ONE_MILLION;
    }

    @Override
    public long getMinCallTime() {
        // Return 0 if minCall hasn't been set yet.
        if (minCall == Long.MAX_VALUE) {
            return 0;
        }
        return minCall / ONE_MILLION;
    }

    @Override
    public double getMedianTimeRequestInMillis() {
        return getPercentileMillis(50);
    }
}
