/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.keyvalue.cassandra.paging;

public class CellPagerBatchSizingStrategy {
    public PageSizes computePageSizes(int cellBatchHint, StatsAccumulator stats) {
        if (stats.count() == 0) {
            // No data yet: go with a square
            return negotiateExactPageSize(Math.round(Math.sqrt(cellBatchHint)), cellBatchHint);
        } else {
            return computePageSizesAssumingNormalDistribution(
                    cellBatchHint, stats.mean(), stats.populationStandardDeviation());
        }
    }

    /* Given the batch size in cells supplied by the user, we want to figure out the optimal (or at least decent)
     * batching parameters for Cassandra. The `get_range_slices' call allows us to load a "rectangle"
     * of cells at once:
     *
     *         |<-w=4->|
     *         ._______.      ___
     *   Row 1 |x x x x|       ^       (each 'x' is a (cell, ts) pair, i.e. a single "column" in Cassandra)
     *   Row 2 |x x    |       |
     *   Row 3 |x x x  |      h=5
     *   Row 4 |x x x x|x x    |
     *   Row 5 |x______|      _v_
     *   Row 6  x x
     *   Row 7  x x x x
     *   Row 8  x x x x x
     *        ...
     *   Row N  x x
     *
     * Here, the height of the rectangle "h" is KeyRange.count and the width "w" is SliceRange.count.
     * We want the area of our rectangle to be on the order of "cellBatchHint" supplied by the user, i.e.
     *
     *     w * h ~ cellBatchHint                  (*)
     *
     * The question is how to choose "w" and "h" appropriately. If a row contains "w" or more columns (like rows 1 and
     * 4 in the picture above), we need to make at least one extra round trip to retrieve that row individually
     * using the "get_slice" call. Thus, setting "w" too low will result in extra round trips to retrieve the individual
     * rows, while setting it too high would make "h" low (since "w" and "h" are inversely proportional as in (*)),
     * resulting in more round trips to make progress "vertically".
     *
     * It makes sense to try to minimize the number of round trips to Cassandra. Let's assume that rows will be short
     * enough to fit in "cellBatchHint", i.e. whenever we need to fetch the remainder of a row, we can do so with
     * a single call to "get_slice" with SliceRange.count set to "cellBatchHint". (If the assumption doesn't hold
     * and our rows are wide, then a particular choice of "w" and "h" doesn't have as much impact on the number of round
     * trips -- we need to page through the individual rows anyway).
     *
     * Then for a table with N rows, the number of round trips is approximately equal to
     *
     *     (N / h) + (# of rows with >= w columns)
     *
     * Let F(k) be the cumulative distribution function of the number of columns per row. Also, remember from (*)
     * that "h" is on the order of "cellBatchHint / w". Hence the above expression is approximately equal to
     *
     *     N * w / cellBatchHint + N * (1 - F(w - 1))
     *
     * Minimizing this is equivalent to minimizing
     *
     *     w / cellBatchHint - F(w - 1)            (**)
     *
     * So if we had an adequate model of F, we could estimate its parameters and minimize (**) with respect to "w".
     * The problem is that the nature of F can depend on the format of a particular table. For example, an append-only
     * table with five named columns where all five columns are always written at once for every row, will have
     * a degenerate distribution localized at 5; a single-column table with mutations might exhibit a Poisson-like
     * behavior; while a table with dynamic columns can be shaped according to an arbitrary distribution depending
     * on the user data.
     *
     * One other possibility is to compute a discrete approximation to the PDF/CDF instead of choosing a fixed family
     * of distributions and estimating its parameters, but that seems both excessive and expensive.
     *
     * Pragmatically speaking, we want a formula like
     *
     *     w = < estimated mean # columns per row > + 1 + g(sigma, cellBatchHint)    (***)
     *
     * where g(sigma, cellBatchHint) is some term that grows with variance sigma^2. The term is necessary to efficiently
     * handle both the "append-only fixed-column table" (zero variance) and the "partially dirty table" (high variance)
     * cases.
     *
     * If we minimize (**) in the case of F being the normal distribution, we get (after doing all the calculus):
     *
     *     w = mu + 1 + sigma * sqrt(-2 * ln(sqrt(2*pi*sigma^2) / cellBatchHint))       (****)
     *
     * where "mu" is the mean and "sigma" is the standard deviation. This actually looks like (***) for sufficiently
     * small values of sigma, with
     *
     *    g(sigma, cellBatchHint) = sigma * sqrt(-2 * ln(sqrt(2*pi*sigma^2) / cellBatchHint))     (*****)
     *
     * Of course, a real table can't have normally distributed row widths (simply because the
     * row width is a discrete quantity), but:
     *
     *      - the normal distribution approximates the "clean narrow table" (low variance) case quite well,
     *        and is a half-decent approximation to the Poisson distribution (the "single-column dirty" case);
     *      - it's easy to estimate the parameters for it (mu and sigma), as well as to do the calculus exercise
     *        to get (****);
     *      - the numbers it produces actually seem reasonable.
     *
     * To illustrate the last point, here is a table of g(sigma, cellBatchHint) for some values of sigma. (The physical
     * meaning of "g" is the number of extra columns to fetch per row in addition to the mean number of columns per row)
     *
     *      cellBatchHint = 1000            cellBatchHint = 100
     *
     *       sigma  | g(sigma, 1000)         sigma  | g(sigma, 100)
     *      --------+---------------        --------+--------------
     *       0      |   0.00                 0      |  0.00
     *       0.5    |   1.82                 0.5    |  1.48
     *       1      |   3.46                 1      |  2.72
     *       2      |   6.51                 2      |  4.89
     *       5      |  14.80                 5      | 10.19
     *       10     |  27.15                 10     | 16.64
     *       20     |  48.93
     *       50     | 101.90
     *       100    | 166.35
     */
    private static PageSizes computePageSizesAssumingNormalDistribution(
                int cellBatchHint,
                double columnsPerRowMean,
                double columnsPerRowStdDev) {
        double sigma = Math.min(columnsPerRowStdDev, cellBatchHint * 0.2);
        // If sigma > cellBatchHint/sqrt(2*pi), then the formula doesn't work.
        // At that point, variance in row length is so big that we can't do much:
        // just use the minimum columnPerRowLimit to at least minimize the number of calls
        // to get_range_slice.
        double logArg = Math.min(1.0, SQRT_2_PI * sigma / cellBatchHint);
        // "extraColumnsToFetch" is the value of "g" defined in (*****)
        // Mathematically, the limit of x*ln(x) is 0 as x->0, but numerically we need to compute the logarithm
        // separately (unless we use a Taylor approximation to x*ln(x) instead, which we don't want),
        // so we handle the case of small "s" separately to avoid the logarithm blow up at 0.
        double extraColumnsToFetch = logArg < 1e-12 ? 0.0 : sigma * Math.sqrt(-2.0 * Math.log(logArg));
        return negotiateExactPageSize(columnsPerRowMean + 1.0 + extraColumnsToFetch, cellBatchHint);
    }

    private static PageSizes negotiateExactPageSize(double desiredColumnPerRowLimit, int cellBatchHint) {
        int columnPerRowLimit1 = Math.max(2, Math.min(cellBatchHint, (int) Math.round(desiredColumnPerRowLimit)));
        int rowLimit = Math.max(1, cellBatchHint / columnPerRowLimit1);
        int columnPerRowLimit2 = Math.max(2, cellBatchHint / rowLimit);
        return new PageSizes(rowLimit, columnPerRowLimit2);
    }

    public static class PageSizes {
        final int rowLimit;
        final int columnPerRowLimit;

        public PageSizes(int rowLimit, int columnPerRowLimit) {
            this.rowLimit = rowLimit;
            this.columnPerRowLimit = columnPerRowLimit;
        }
    }

    private static final double SQRT_2_PI = Math.sqrt(2.0 * Math.PI);

}
