/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.watch;

import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Set;

public class NoOpLockWatchValueCache implements LockWatchValueCache {
    private static final SafeLogger log = SafeLoggerFactory.get(NoOpLockWatchValueCache.class);

    public static LockWatchValueCache create() {
        return new NoOpLockWatchValueCache();
    }

    @Override
    public void processStartTransactions(Set<Long> startTimestamps) {}

    @Override
    public void updateCacheWithCommitTimestampsInformation(Set<Long> startTimestamps) {}

    @Override
    public void requestStateRemoved(long startTimestamp) {}

    @Override
    public void ensureStateRemoved(long startTimestamp) {}

    @Override
    public void onSuccessfulCommit(long startTimestamp) {}

    @Override
    public void logState() {
        log.info("Logging state from NoOpLockWatchValueCache");
    }

    @Override
    public void close() {}
}
