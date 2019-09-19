/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.timestamp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.logsafe.SafeArg;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class PersistentTimestampServiceImpl implements PersistentTimestampService {
    private class InitializingWrapper extends AsyncInitializer implements AutoDelegate_PersistentTimestampService {
        @Override
        public PersistentTimestampService delegate() {
            checkInitialized();
            return PersistentTimestampServiceImpl.this;
        }

        @Override
        protected void tryInitialize() {
            PersistentTimestampServiceImpl.this.tryInitialize();
        }

        @Override
        protected String getInitializingClassName() {
            return "PersistentTimestampService";
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentTimestampServiceImpl.class);
    private static final int MAX_TIMESTAMPS_PER_REQUEST = 10_000;

    private ErrorCheckingTimestampBoundStore store;
    private PersistentTimestamp timestamp;
    private final InitializingWrapper wrapper = new InitializingWrapper();

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
        service.wrapper.initialize(initializeAsync);
        return service.wrapper.isInitialized() ? service : service.wrapper;
    }

    @VisibleForTesting
    PersistentTimestampServiceImpl(PersistentTimestamp timestamp) {
        this.timestamp = timestamp;
    }

    private PersistentTimestampServiceImpl(ErrorCheckingTimestampBoundStore store) {
        this.store = store;
    }

    private void tryInitialize() {
        long latestTimestamp = store.getUpperLimit();
        PersistentUpperLimit upperLimit = new PersistentUpperLimit(store);
        timestamp = new PersistentTimestamp(upperLimit, latestTimestamp);
    }

    @Override
    public boolean isInitialized() {
        return wrapper.isInitialized();
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
        log.info("Fast-forwarding the timestamp service to timestamp {}.", SafeArg.of("timestamp", newTimestamp));
        timestamp.increaseTo(newTimestamp);
    }

    @Override
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
