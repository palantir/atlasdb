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

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This is a logger intended for use tracking down problems arising from
 * https://github.com/palantir/atlasdb/issues/1000.  To activate this logging
 * please follow instructions on
 * http://palantir.github.io/atlasdb/html/configuration/logging.html#debug-logging-for-multiple-timestamp-services-error
 */
@SuppressFBWarnings("SLF4J_LOGGER_SHOULD_BE_PRIVATE")
public final class DebugLogger {
    public static final SafeLogger logger = SafeLoggerFactory.get(DebugLogger.class);

    private DebugLogger() {
        // Logging utility class
    }

    public static void handedOutTimestamps(TimestampRange range) {
        long count = range.getUpperBound() - range.getLowerBound() + 1L;
        logger.trace(
                "Handing out {} timestamps, taking us to {}.",
                SafeArg.of("count", count),
                SafeArg.of("rangeUpperBound", range.getUpperBound()));
    }

    public static void createdPersistentTimestamp() {
        logger.info(
                "Creating PersistentTimestamp object on thread {}."
                        + " If you are running embedded AtlasDB, this should only happen once."
                        + " If you are using Timelock, this should happen once per client per leadership election",
                UnsafeArg.of("threadName", Thread.currentThread().getName()));
    }

    public static void willStoreNewUpperLimit(long newLimit) {
        logger.trace("storing new upper limit: {}.", SafeArg.of("newLimit", newLimit));
    }

    public static void didStoreNewUpperLimit(long newLimit) {
        logger.trace("Stored; upper limit is now {}.", SafeArg.of("newLimit", newLimit));
    }
}
