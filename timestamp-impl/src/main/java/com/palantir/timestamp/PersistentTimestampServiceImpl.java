/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.timestamp;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.exception.NotInitializedException;
import com.palantir.processors.AutoDelegate;

@AutoDelegate(typeToExtend = PersistentTimestampService.class)
@ThreadSafe
public class PersistentTimestampServiceImpl implements AsyncInitializer, PersistentTimestampService {

    private volatile boolean isInitialized = false;
    private ErrorCheckingTimestampBoundStore store;

    public static class InitializingWrapper implements AutoDelegate_PersistentTimestampService {

        private PersistentTimestampServiceImpl service;
        public InitializingWrapper(PersistentTimestampServiceImpl service) {
            this.service = service;
        }

        @Override
        public PersistentTimestampService delegate() {
            if (service.isInitialized()) {
                return service;
            }
            throw new NotInitializedException("PersistentTimestampService");
        }

    }

    private static final int MAX_TIMESTAMPS_PER_REQUEST = 10_000;

    private PersistentTimestamp timestamp;

    public static PersistentTimestampService create(TimestampBoundStore store) {
        return create(new ErrorCheckingTimestampBoundStore(store), AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static PersistentTimestampService create(TimestampBoundStore store, boolean initializeAsync) {
        return create(new ErrorCheckingTimestampBoundStore(store), initializeAsync);
    }

    public static PersistentTimestampService create(ErrorCheckingTimestampBoundStore store) {
        return create(store, AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static PersistentTimestampService create(ErrorCheckingTimestampBoundStore store,
            boolean initializeAsync) {
        PersistentTimestampServiceImpl service = new PersistentTimestampServiceImpl(store);
        service.initialize(initializeAsync);
        return service.isInitialized() ? service : new InitializingWrapper(service);
    }

    @VisibleForTesting
    PersistentTimestampServiceImpl(PersistentTimestamp timestamp) {
        this.timestamp = timestamp;
    }

    private PersistentTimestampServiceImpl(ErrorCheckingTimestampBoundStore store) {
        this.store = store;
    }

    @Override
    public boolean isInitialized() {
        return isInitialized;
    }

    @Override
    public synchronized void tryInitialize() {
        long latestTimestamp = store.getUpperLimit();
        PersistentUpperLimit upperLimit = new PersistentUpperLimit(store);
        this.timestamp = new PersistentTimestamp(upperLimit, latestTimestamp);
        isInitialized = true;
    }

    @Override
    public void cleanUpOnInitFailure() {
        // no-op
    }

    @Override
    public long getFreshTimestamp() {
        return getFreshTimestamps(1).getLowerBound();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        int numTimestampsToReturn = cleanUpTimestampRequest(numTimestampsRequested);

        TimestampRange range = timestamp.incrementBy(numTimestampsToReturn);
        DebugLogger.handedOutTimestamps(range);
        return range;
    }

    @Override
    public void fastForwardTimestamp(long newTimestamp) {
        checkFastForwardRequest(newTimestamp);
        timestamp.increaseTo(newTimestamp);
    }

    @SuppressWarnings("unused") // used by product
    public long getUpperLimitTimestampToHandOutInclusive() {
        return timestamp.getUpperLimitTimestampToHandOutInclusive();
    }

    private void checkFastForwardRequest(long newTimestamp) {
        Preconditions.checkArgument(newTimestamp != TimestampManagementService.SENTINEL_TIMESTAMP,
                "Cannot fast forward to the sentinel timestamp %s. If you accessed this timestamp service remotely"
                        + " this is likely due to specifying an incorrect query parameter.", newTimestamp);
    }

    private static int cleanUpTimestampRequest(int numTimestampsRequested) {
        if (numTimestampsRequested <= 0) {
            // explicitly not using Preconditions to optimize hot success path and avoid allocations
            throw new IllegalArgumentException(String.format(
                    "Number of timestamps requested must be greater than zero, was %s", numTimestampsRequested));

        }
        return Math.min(numTimestampsRequested, MAX_TIMESTAMPS_PER_REQUEST);
    }

    @Override
    public String ping() {
        return PING_RESPONSE;
    }
}
