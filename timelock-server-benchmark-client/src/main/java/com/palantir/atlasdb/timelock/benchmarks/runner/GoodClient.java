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
        runAndPrintResults(() -> client1.readTransactionRows(4, 100, 100, 100, 0));
    }

    @Test
    public void rowsRangeScan() {
        runAndPrintResults(() -> client1.rangeScanRows(1, 100, 100, 100, 0, 0));
    }

    @Test
    public void dynamicColumnsRangeScan() {
        runAndPrintResults(() -> client1.rangeScanDynamicColumns(1, 100, 100, 100, 0, 0));
    }

    @Test
    public void writeTransactionRows() {
        runAndPrintResults(() -> client1.writeTransactionRows(1, 10, 10, 100));
    }

}

/*
{
  "average" : 5.9344725,
  "p99" : 16.867851,
  "numRows" : 100,
  "totalTime" : 593.86237,
  "name" : "RowsReadTransactionBenchmark",
  "requestsPerClient" : 100,
  "dataSize" : 100,
  "p50" : 5.603871,
  "numClients" : 1,
  "throughput" : 168.38918418083975,
  "numUpdatesPerCell" : 0,
  "p95" : 7.831217
}

{
  "average" : 5.73880764,
  "numRows" : 100,
  "totalTime" : 574.399449,
  "dataSize" : 100,
  "p50" : 5.624224,
  "numDeleted" : 0,
  "p95" : 6.702651,
  "numUpdates" : 0,
  "p99" : 7.43809,
  "name" : "RowsRangeScanBenchmark",
  "requestsPerClient" : 100,
  "numClients" : 1,
  "throughput" : 174.0948745234608
}

{
  "average" : 4.419869940000001,
  "numRows" : 100,
  "totalTime" : 442.431899,
  "dataSize" : 100,
  "p50" : 4.317984,
  "numDeleted" : 0,
  "p95" : 5.167763,
  "numUpdates" : 0,
  "p99" : 6.277169,
  "name" : "DynamicColumnsRangeScanBenchmark",
  "requestsPerClient" : 100,
  "numClients" : 1,
  "throughput" : 226.02348570711897
}

{
  "average" : 35.786848,
  "p99" : 39.818042,
  "totalTime" : 358.095898,
  "name" : "RowsWriteTransactionBenchmark",
  "requestsPerClient" : 10,
  "p50" : 35.439482,
  "numClients" : 1,
  "throughput" : 27.92548045328349,
  "p95" : 39.818042
}

 */
