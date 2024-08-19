/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.transaction.api.expectations.ExpectationsData;
import com.palantir.atlasdb.transaction.expectations.ExpectationsMetrics;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;

/**
 * This enum exists we don't have to enable full SnapshotTransaction debug logging to get expectations metrics.
 * It is possible to nest inside SnapshotTransaction, but the class is already quite large.
 */
enum ExpectationsMetricsReporter {
    INSTANCE;

    private static final SafeLogger log = SafeLoggerFactory.get(ExpectationsMetricsReporter.class);

    void reportExpectationsCollectedData(ExpectationsData expectationsData, ExpectationsMetrics metrics) {
        if (log.isDebugEnabled()) {
            reportExpectationsCollectedDataInternal(expectationsData, metrics);
        }
    }

    private void reportExpectationsCollectedDataInternal(
            ExpectationsData expectationsData, ExpectationsMetrics metrics) {
        metrics.ageMillis().update(expectationsData.ageMillis());
        metrics.bytesRead().update(expectationsData.readInfo().bytesRead());
        metrics.kvsReads().update(expectationsData.readInfo().kvsCalls());

        expectationsData
                .readInfo()
                .maximumBytesKvsCallInfo()
                .ifPresent(kvsCallReadInfo ->
                        metrics.mostKvsBytesReadInSingleCall().update(kvsCallReadInfo.bytesRead()));

        metrics.cellCommitLocksRequested()
                .update(expectationsData.commitLockInfo().cellCommitLocksRequested());
        metrics.rowCommitLocksRequested()
                .update(expectationsData.commitLockInfo().rowCommitLocksRequested());
        metrics.changeMetadataBuffered()
                .update(expectationsData.writeMetadataInfo().changeMetadataBuffered());
        metrics.cellChangeMetadataSent()
                .update(expectationsData.writeMetadataInfo().cellChangeMetadataSent());
        metrics.rowChangeMetadataSent()
                .update(expectationsData.writeMetadataInfo().rowChangeMetadataSent());
    }
}
