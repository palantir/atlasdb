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

package com.palantir.lock.client.timestampleases;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureTimestampRange;
import com.palantir.lock.client.NamespacedConjureTimelockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.TimestampRange;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.jetbrains.annotations.VisibleForTesting;

final class ExactTimestampRangeSupplier {
    private static final SafeLogger log = SafeLoggerFactory.get(ExactTimestampRangeSupplier.class);

    private static final Retryer<TimestampRange> RETRYER = RetryerBuilder.<TimestampRange>newBuilder()
            .retryIfException()
            .withStopStrategy(StopStrategies.stopAfterAttempt(3))
            .build();

    private ExactTimestampRangeSupplier() {}

    static List<TimestampRange> getFreshTimestamps(int requested, NamespacedConjureTimelockService service) {
        return getFreshTimestamps(requested, count -> getFreshTimestampRange(count, service));
    }

    @VisibleForTesting
    static List<TimestampRange> getFreshTimestamps(
            int requested, Function<Integer, TimestampRange> freshTimestampsSupplier) {
        List<TimestampRange> ranges = new ArrayList<>();
        long obtained = 0;
        while (obtained < requested) {
            int remaining = Ints.checkedCast(requested - obtained);
            TimestampRange newRange = getFreshTimestampRangeWithRetry(remaining, freshTimestampsSupplier);

            ranges.add(newRange);
            obtained += newRange.size();
        }

        return ranges;
    }

    private static TimestampRange getFreshTimestampRangeWithRetry(
            int requested, Function<Integer, TimestampRange> freshTimestampSupplier) {
        try {
            return RETRYER.call(() -> freshTimestampSupplier.apply(requested));
        } catch (ExecutionException e) {
            log.info("Failed attempt to get fresh timestamps. This should be transient.", e.getCause());
            throw new SafeRuntimeException("Failed attempt to get fresh timestamps. This should be transient.", e);
        } catch (RetryException e) {
            log.info(
                    "Exhausted attempts to get fresh timestamps. This should be transient.",
                    SafeArg.of("numberOfFailedAttempts", e.getNumberOfFailedAttempts()),
                    e);
            throw new SafeRuntimeException("Exhausted attempts to get fresh timestamps. This should be transient.", e);
        }
    }

    private static TimestampRange getFreshTimestampRange(int requested, NamespacedConjureTimelockService service) {
        ConjureTimestampRange conjureRange = service.getFreshTimestampsV2(
                        ConjureGetFreshTimestampsRequestV2.of(requested))
                .get();
        return TimestampRange.createRangeFromDeltaEncoding(conjureRange.getStart(), conjureRange.getCount());
    }
}
