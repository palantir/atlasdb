/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.expectations;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;

/**
 * Forwarding {@link KeyValueServiceDataTracker} which handles exceptions for record calls.
 */
public final class FailSafeKeyValueServiceDataTracker implements KeyValueServiceDataTracker {
    private static final SafeLogger log = SafeLoggerFactory.get(FailSafeKeyValueServiceDataTracker.class);

    private final KeyValueServiceDataTracker delegate;

    public FailSafeKeyValueServiceDataTracker(KeyValueServiceDataTracker delegate) {
        this.delegate = delegate;
    }

    @Override
    public TransactionReadInfo getReadInfo() {
        return delegate.getReadInfo();
    }

    @Override
    public ImmutableMap<TableReference, TransactionReadInfo> getReadInfoByTable() {
        return delegate.getReadInfoByTable();
    }

    @Override
    public void recordReadForTable(TableReference tableRef, String methodName, long bytesRead) {
        try {
            delegate.recordReadForTable(tableRef, methodName, bytesRead);
        } catch (Exception exception) {
            log.error("Data tracking (recordReadForTable) failed", exception);
        }
    }

    @Override
    public void recordTableAgnosticRead(String methodName, long bytesRead) {
        try {
            delegate.recordTableAgnosticRead(methodName, bytesRead);
        } catch (Exception exception) {
            log.error("Data tracking (recordTableAgnosticRead) failed", exception);
        }
    }

    @Override
    public BytesReadTracker recordCallForTable(TableReference tableRef) {
        BytesReadTracker tracker = _unused -> {};

        try {
            tracker = wrapForExceptionHandling(delegate.recordCallForTable(tableRef));
        } catch (Exception exception) {
            log.error("Data tracking (recordCallForTable) failed", exception);
        }

        return tracker;
    }

    private static BytesReadTracker wrapForExceptionHandling(BytesReadTracker tracker) {
        return bytesRead -> {
            try {
                tracker.record(bytesRead);
            } catch (Exception exception) {
                log.error("Data tracking (ByteReadTracker record) failed", exception);
            }
        };
    }
}
