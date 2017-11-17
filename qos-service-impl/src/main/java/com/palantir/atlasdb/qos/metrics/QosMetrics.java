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

package com.palantir.atlasdb.qos.metrics;

import com.codahale.metrics.Meter;
import com.palantir.atlasdb.qos.QueryWeight;
import com.palantir.atlasdb.util.MetricsManager;

// TODO(nziebart): needs tests
public class QosMetrics {
    private final MetricsManager metricsManager = new MetricsManager();

    private final Meter readRequestCount;
    private final Meter bytesRead;
    private final Meter readTime;
    private final Meter rowsRead;

    private final Meter writeRequestCount;
    private final Meter bytesWritten;
    private final Meter writeTime;
    private final Meter rowsWritten;


    public QosMetrics() {
        readRequestCount = metricsManager.registerMeter(QosMetrics.class, "numReadRequests");
        bytesRead = metricsManager.registerMeter(QosMetrics.class, "bytesRead");
        readTime = metricsManager.registerMeter(QosMetrics.class, "readTime");
        rowsRead = metricsManager.registerMeter(QosMetrics.class, "rowsRead");

        writeRequestCount = metricsManager.registerMeter(QosMetrics.class, "numWriteRequests");
        bytesWritten = metricsManager.registerMeter(QosMetrics.class, "bytesWritten");
        writeTime = metricsManager.registerMeter(QosMetrics.class, "writeTime");
        rowsWritten = metricsManager.registerMeter(QosMetrics.class, "rowsWritten");
    }

    public void recordRead(QueryWeight weight) {
        readRequestCount.mark();
        bytesRead.mark(weight.numBytes());
        readTime.mark(weight.timeTakenMicros());
        rowsRead.mark(weight.numDistinctRows());
    }

    public void recordWrite(QueryWeight weight) {
        writeRequestCount.mark();
        bytesWritten.mark(weight.numBytes());
        writeTime.mark(weight.timeTakenMicros());
        rowsWritten.mark(weight.numDistinctRows());
    }

}
