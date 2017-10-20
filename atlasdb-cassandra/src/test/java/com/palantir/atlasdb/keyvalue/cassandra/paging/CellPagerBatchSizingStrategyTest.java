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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.palantir.atlasdb.keyvalue.cassandra.paging.CellPagerBatchSizingStrategy.PageSizes;

public class CellPagerBatchSizingStrategyTest {

    private final CellPagerBatchSizingStrategy strategy = new CellPagerBatchSizingStrategy();

    @Test
    public void testUseSquareIfNoStats() {
        PageSizes pageSizes = strategy.computePageSizes(102, stats());
        assertEquals(10, pageSizes.columnPerRowLimit);
        assertEquals(10, pageSizes.rowLimit);
    }

    @Test
    public void testZeroVariance() {
        PageSizes pageSizes = strategy.computePageSizes(100, stats(4, 4, 4, 4, 4));
        assertEquals(5, pageSizes.columnPerRowLimit);
        assertEquals(20, pageSizes.rowLimit);
    }

    @Test
    public void testNonZeroVariance() {
        PageSizes pageSizes = strategy.computePageSizes(100, stats(4, 5, 4, 5, 4, 5));
        assertTrue(pageSizes.columnPerRowLimit > 5);
        assertTrue(pageSizes.rowLimit < 20);
    }

    @Test
    public void testReturnReasonableResults() {
        for (double v = 1; v <= 200; v += 0.1) {
            PageSizes pageSizes = strategy.computePageSizes(100, stats(1, 1, 1, v, v, v));
            assertTrue(pageSizes.rowLimit > 0);
            assertTrue(pageSizes.rowLimit <= 100);
            assertTrue(pageSizes.columnPerRowLimit >= 2);
            assertTrue(pageSizes.columnPerRowLimit <= 100);
            assertTrue(pageSizes.rowLimit * pageSizes.columnPerRowLimit <= 100);
        }
    }

    private StatsAccumulator stats(double... measurements) {
        StatsAccumulator stats = new StatsAccumulator();
        for (double x : measurements) {
            stats.add(x);
        }
        return stats;
    }

}
