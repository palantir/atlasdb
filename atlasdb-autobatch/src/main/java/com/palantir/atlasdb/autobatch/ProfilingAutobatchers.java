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
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;

import com.google.common.util.concurrent.RateLimiter;
import com.palantir.logsafe.SafeArg;

public final class ProfilingAutobatchers {

    private static final Duration AUTOBATCHER_LOGGING_INTERVAL = Duration.ofSeconds(10);
    private static final double AUTOBATCHER_LOGGING_PERMITS_PER_SECOND = 1. / AUTOBATCHER_LOGGING_INTERVAL.getSeconds();

    private ProfilingAutobatchers() {
        // factory
    }

    public static <T, R> DisruptorAutobatcher<T, R> create(
            Logger logger,
            String safeIdentifier,
            Consumer<List<BatchElement<T, R>>> batchFunction) {
        RateLimiter profilingLimiter = RateLimiter.create(AUTOBATCHER_LOGGING_PERMITS_PER_SECOND); // every 10 seconds
        return DisruptorAutobatcher.create(elements -> {
            batchFunction.accept(elements);

            if (profilingLimiter.tryAcquire()) {
                logger.info("Autobatcher with ID {} just processed a batch of size {}."
                                + " This message is rate limited to once every 10 seconds per autobatcher, so there"
                                + " may be more batches being processed.",
                        SafeArg.of("autobatcherIdentifier", safeIdentifier),
                        SafeArg.of("batchSize", elements.size()));
            }
        });
    }
}
