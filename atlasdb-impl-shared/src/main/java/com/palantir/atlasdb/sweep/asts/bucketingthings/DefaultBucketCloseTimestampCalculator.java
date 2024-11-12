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

package com.palantir.atlasdb.sweep.asts.bucketingthings;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.atlasdb.cleaner.PuncherStore;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.Supplier;

public final class DefaultBucketCloseTimestampCalculator {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultBucketCloseTimestampCalculator.class);

    @VisibleForTesting
    static final Duration TIME_GAP_BETWEEN_BUCKET_START_AND_END = Duration.ofMinutes(10);

    @VisibleForTesting
    static final long MIN_BUCKET_SIZE = SweepQueueUtils.TS_COARSE_GRANULARITY;

    @VisibleForTesting
    static final long MAX_BUCKET_SIZE_FOR_NON_PUNCHER_CLOSE = 500 * MIN_BUCKET_SIZE; // 5 billion

    private final PuncherStore puncherStore;
    private final Supplier<Long> freshTimestampSupplier;
    private final Clock clock;

    @VisibleForTesting
    DefaultBucketCloseTimestampCalculator(
            PuncherStore puncherStore, Supplier<Long> freshTimestampSupplier, Clock clock) {
        this.puncherStore = puncherStore;
        this.freshTimestampSupplier = freshTimestampSupplier;
        this.clock = clock;
    }

    public static DefaultBucketCloseTimestampCalculator create(
            PuncherStore puncherStore, TimelockService timelockService) {
        return new DefaultBucketCloseTimestampCalculator(
                puncherStore, timelockService::getFreshTimestamp, Clock.systemUTC());
    }

    // A second possible algorithm, rather than the fixed bounds above, is to (assuming the start timestamp is X):
    //
    // * Get your node’s wallclock time (A), and the wallclock time associated with the start timestamp (B).
    // * Get the latest timestamp from timelock, Y.
    // * Calculate B - A to get the number of minutes passed, then divide by 10 to get the number of 10 minute blocks
    // * The end timestamp is X + [(Y - X) / number of blocks]
    //
    //
    // This is essentially performing a linear interpolation, assuming the timestamps are uniformly distributed across
    // the window from A → B (which is almost certainly not the case, but a good approximation).
    //
    // We’re explicitly not doing this algorithm now given the added complexity, but this may be implemented if the
    // fixed parameters are too coarse.
    public OptionalLong getBucketCloseTimestamp(long startTimestamp) {
        Preconditions.checkArgument(
                startTimestamp == clampTimestampToCoarsePartitionBoundary(startTimestamp),
                "startTimestamp must be on a coarse partition boundary",
                SafeArg.of("startTimestamp", startTimestamp));
        long openWallClockTimeMillis = puncherStore.getMillisForTimestamp(startTimestamp);
        long closeWallClockTimeMillis = openWallClockTimeMillis + TIME_GAP_BETWEEN_BUCKET_START_AND_END.toMillis();

        if (Instant.now(clock).toEpochMilli() < closeWallClockTimeMillis) {
            return OptionalLong.empty();
        }

        long finalLogicalTimestamp = puncherStore.get(closeWallClockTimeMillis);
        long clampedTimestamp = clampTimestampToCoarsePartitionBoundary(finalLogicalTimestamp);

        // if this case happens, it's possibly clockdrift or a delayed write, or we just make too slow progress
        // TODO(mdaudali): Re-evaluate if the lower bound of 10_000_000 timestamps per 10 mins makes sense.
        // This has the following interesting case: Suppose that the assigner stopped for a day. Then, suppose that the
        // within the following 10 minute period from where the assigner currently is looking at, there is _not_ 10 mil
        // timestamps. Then, instead of choosing a 10 million sized block, we have a giant block up to the fresh Ts.
        // This is capped at 5 billion, and we were okay with this case when there's clock drift, but we should
        // evaluate.
        if (clampedTimestamp - startTimestamp < MIN_BUCKET_SIZE) {
            long freshClampedTimestamp = clampTimestampToCoarsePartitionBoundary(freshTimestampSupplier.get());
            if (freshClampedTimestamp - startTimestamp < MIN_BUCKET_SIZE) {
                logNonPuncherClose(
                        "but this is not sufficiently far from the start timestamp to close the bucket.",
                        finalLogicalTimestamp,
                        startTimestamp,
                        openWallClockTimeMillis,
                        freshClampedTimestamp);
                return OptionalLong.empty();
            }
            // start timestamp is aligned on a coarse partition
            // MAX_BUCKET_SIZE is also a multiple of a coarse partition
            // so excluding overflow, start + MAX will also be aligned with a coarse partition.
            long cappedTimestamp =
                    Math.min(startTimestamp + MAX_BUCKET_SIZE_FOR_NON_PUNCHER_CLOSE, freshClampedTimestamp);
            if (cappedTimestamp != freshClampedTimestamp) {
                logNonPuncherClose(
                        "but this is too far from the start timestamp. Proposing a capped timestamp {} instead.",
                        finalLogicalTimestamp,
                        startTimestamp,
                        openWallClockTimeMillis,
                        freshClampedTimestamp,
                        SafeArg.of("cappedTimestamp", cappedTimestamp));
            } else {
                logNonPuncherClose(
                        "and this is sufficiently far from the start timestamp to close the bucket.",
                        finalLogicalTimestamp,
                        startTimestamp,
                        openWallClockTimeMillis,
                        freshClampedTimestamp);
            }
            return OptionalLong.of(cappedTimestamp);
        } else {
            return OptionalLong.of(clampedTimestamp);
        }
    }

    private long clampTimestampToCoarsePartitionBoundary(long timestamp) {
        return SweepQueueUtils.minTsForCoarsePartition(SweepQueueUtils.tsPartitionCoarse(timestamp));
    }

    private void logNonPuncherClose(
            @CompileTimeConstant String logMessageSuffix,
            long finalTimestampFromPunchTable,
            long startTimestamp,
            long openWallClockTimeMillis,
            long freshClampedTimestamp,
            Arg<?>... additionalArgs) {
        List<Arg<?>> args = ImmutableList.<Arg<?>>builder()
                .add(
                        SafeArg.of("finalTimestampFromPunchTable", finalTimestampFromPunchTable),
                        SafeArg.of("startTimestamp", startTimestamp),
                        SafeArg.of("timeGap", TIME_GAP_BETWEEN_BUCKET_START_AND_END),
                        SafeArg.of("openWallClockTimeMillis", openWallClockTimeMillis),
                        SafeArg.of("freshClampedTimestamp", freshClampedTimestamp))
                .addAll(Arrays.asList(additionalArgs))
                .build();
        log.info(
                "Read a logical timestamp {} from the puncher store that's not sufficiently far from the start"
                        + " timestamp {}, despite requesting a time {} after the start timestamp's associated wall"
                        + " clock time {}. This is likely due to some form of clock drift, but should not be happening"
                        + " repeatedly.  We  read a fresh timestamp that has been clamped down to the nearest coarse"
                        + " partition boundary {}, " + logMessageSuffix,
                args);
    }
}
