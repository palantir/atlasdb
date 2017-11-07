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

package com.palantir.atlasdb.timelock.benchmarks.runner;

import org.junit.Test;

public class GoodClient extends BenchmarkRunnerBase {

    @Test
    public void readTransactionRows() {
        runAndPrintResults(() -> client.readTransactionRows(100, 100, 100, 100, 1));
    }

    @Test
    public void rowsRangeScan() {
        runAndPrintResults(() -> client.rangeScanRows(100, 100, 100, 100, 1, 0));
    }

    @Test
    public void dynamicColumnsRangeScan() {
        runAndPrintResults(() -> client.rangeScanDynamicColumns(64, 50, 100, 100, 1, 0));
    }

}
