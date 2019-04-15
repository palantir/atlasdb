/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.autobatch;

import java.time.Duration;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;
import com.palantir.logsafe.SafeArg;

@NotThreadSafe // Disruptor runs the batching function on just one thread.
public final class BatchSizeLogger {
    private static final Logger log = LoggerFactory.getLogger(BatchSizeLogger.class);

    private static final Duration AUTOBATCHER_LOGGING_INTERVAL = Duration.ofSeconds(10);
    private static final double AUTOBATCHER_LOGGING_PERMITS_PER_SECOND = 1. / AUTOBATCHER_LOGGING_INTERVAL.getSeconds();

    private final RateLimiter rateLimiter;
    private final String safeIdentifier;

    private long total = 0;
    private long counter = 0;

    private BatchSizeLogger(RateLimiter rateLimiter, String safeIdentifier) {
        this.rateLimiter = rateLimiter;
        this.safeIdentifier = safeIdentifier;
    }

    public static BatchSizeLogger create(String safeLoggerIdentifier) {
        RateLimiter profilingLimiter = RateLimiter.create(AUTOBATCHER_LOGGING_PERMITS_PER_SECOND); // every 10 seconds
        return new BatchSizeLogger(profilingLimiter, safeLoggerIdentifier);
    }

    public void markBatchProcessed(long batchSize) {
        total += batchSize;
        counter++;
        if (rateLimiter.tryAcquire()) {
            log.info("The autobatcher with identifier {} has just processed a batch of size {}."
                    + " Over the last {} seconds, it has processed {} batches with average size {}.",
                    SafeArg.of("safeIdentifier", safeIdentifier),
                    SafeArg.of("currentBatchSize", batchSize),
                    SafeArg.of("interval", AUTOBATCHER_LOGGING_INTERVAL.getSeconds()),
                    SafeArg.of("numBatches", counter),
                    SafeArg.of("averageSize", (double) total / counter));
            total = 0;
            counter = 0;
        }
    }
}
