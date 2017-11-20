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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.codahale.metrics.Meter;
import com.palantir.atlasdb.util.MetricsManager;

public class QosMetrics {
    private final MetricsManager metricsManager = new MetricsManager();

    private final Meter readRequestCount;
    private final Meter writeRequestCount;
    private final Meter bytesRead;
    private final Meter bytesWritten;

    public QosMetrics() {
        readRequestCount = metricsManager.registerOrGetMeter(QosMetrics.class, "numReadRequests");
        writeRequestCount = metricsManager.registerOrGetMeter(QosMetrics.class, "numWriteRequests");

        bytesRead = metricsManager.registerOrGetMeter(QosMetrics.class, "bytesRead");
        bytesWritten = metricsManager.registerOrGetMeter(QosMetrics.class, "bytesWritten");
    }

    public void updateReadCount() {
        readRequestCount.mark();
    }

    public void updateWriteCount() {
        writeRequestCount.mark();
    }

    public void updateBytesRead(long numBytes) {
        bytesRead.mark(numBytes);
    }

    public void updateBytesWritten(long numBytes) {
        bytesWritten.mark(numBytes);
    }
}
