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

/**
 * Note that there is no warmup time included in any of these tests, so if the server has just been started you'll want
 * to execute many requests until the results stabilize (give the JIT compiler time to optimize).
 */
public class BenchmarksRunner extends BenchmarkRunnerBase {

    @Test
    public void timestamp() {
        runAndPrintResults(client1::timestamp, 4, 1000);
    }

    @Test
    public void lockAndUnlockUncontended() {
        runAndPrintResults(client1::lockAndUnlockUncontended, 4, 500);
    }

    @Test
    public void lockAndUnlockContended() {
        runAndPrintResults(() -> client1.lockAndUnlockContended(8, 1000, 2));
    }

    @Test
    public void writeTransactionRows() {
        runAndPrintResults(() -> client1.writeTransactionRows(1, 20, 1000, 500));
    }

    @Test
    public void writeTransactionDynamicColumns() {
        runAndPrintResults(() -> client1.writeTransactionDynamicColumns(1, 20, 1000, 500));
    }

    @Test
    public void kvsCas() {
        runAndPrintResults(client1::kvsCas, 1, 5000);
    }

    @Test
    public void kvsWrite() {
        runAndPrintResults(client1::kvsWrite, 1, 1000);
    }

    @Test
    public void kvsRead() {
        runAndPrintResults(client1::kvsRead, 1, 5000);
    }

    @Test
    public void contendedWriteTransaction() {
        runAndPrintResults(client1::contendedWriteTransaction, 2000, 1);
    }

    @Test
    public void readTransactionRows() {
        runAndPrintResults(() -> client1.readTransactionRows(100, 100, 100, 100, 1));
    }

    @Test
    public void rowsRangeScan() {
        runAndPrintResults(() -> client1.rangeScanRows(100, 100, 100, 100, 1, 0));
    }

    @Test
    public void dynamicColumnsRangeScan() {
        runAndPrintResults(() -> client1.rangeScanDynamicColumns(64, 50, 100, 100, 1, 0));
    }

    @Test
    public void sweep() {
        sweeper1.sweepTableFrom("benchmarks.Blobs", "a2c1c18906749d46b4594ea5790b34e5".toUpperCase());
    }

}

