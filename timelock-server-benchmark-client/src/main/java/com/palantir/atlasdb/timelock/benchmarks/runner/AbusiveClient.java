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

public class AbusiveClient extends BenchmarkRunnerBase {

    @Test
    public void readTransactionRows() {
        runAndPrintResults(() -> client2.readTransactionRows(32, 100, 1000, 10_000, 50));
    }

    @Test
    public void rowsRangeScan() {
        runAndPrintResults(() -> client2.rangeScanRows(1, 100, 1000, 10_000, 50, 250));
    }

    @Test
    public void dynamicColumnsRangeScan() {
        runAndPrintResults(() -> client2.rangeScanDynamicColumns(4, 100, 500, 10_000, 50, 250));
    }

    @Test
    public void writeTransactionRows() {
        runAndPrintResults(() -> client2.writeTransactionRows(32, 100, 500, 10_000));
    }

}
