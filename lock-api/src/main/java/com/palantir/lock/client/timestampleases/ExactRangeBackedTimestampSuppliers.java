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

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.timestamp.TimestampRange;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.function.LongSupplier;

final class ExactRangeBackedTimestampSuppliers {
    private ExactRangeBackedTimestampSuppliers() {}

    /**
     * Returned supplier provides increasing and unique timestamps from a sequence of increasing non-overlapping ranges.
     * If more timestamps are requested than the initially provided count, the supplier will throw.
     */
    static LongSupplier createFromRanges(
            int requested, TimestampRange initialRange, List<TimestampRange> remainingRanges) {
        validateRanges(requested, initialRange, remainingRanges);
        Queue<TimestampRange> ranges = new ArrayDeque<>(1 + remainingRanges.size());
        ranges.add(initialRange);
        ranges.addAll(remainingRanges);
        return new MultipleRangeBackedExactTimestampSupplier(requested, ranges);
    }

    /**
     * Returned supplier provides increasing and unique timestamps from a single range.
     * If more timestamps are requested than the initially provided count, the supplier will throw.
     */
    static LongSupplier createFromRange(TimestampRange range, int requested) {
        validateRange(requested, range);
        return new SingleRangeBackedExactTimestampSupplier(range, requested);
    }

    private static void validateRanges(
            int requested, TimestampRange initialRange, List<TimestampRange> remainingRanges) {
        validateRangesProvideEnoughTimestamps(requested, initialRange, remainingRanges);
        validateRangesAreIncreasingAndNonOverlapping(initialRange, remainingRanges);
    }

    private static void validateRangesAreIncreasingAndNonOverlapping(
            TimestampRange initialRange, List<TimestampRange> ranges) {
        TimestampRange previousRange = initialRange;
        for (TimestampRange currentRange : ranges) {
            Preconditions.checkArgument(
                    previousRange.getUpperBound() < currentRange.getLowerBound(),
                    "Ranges must be increasing and non-overlapping");
            previousRange = currentRange;
        }
    }

    private static void validateRangesProvideEnoughTimestamps(
            int requested, TimestampRange initialRange, List<TimestampRange> remainingRanges) {
        long provided = initialRange.size()
                + remainingRanges.stream().mapToLong(TimestampRange::size).sum();
        Preconditions.checkArgument(provided >= requested, "Not enough timestamps provided");
    }

    private static void validateRange(int requested, TimestampRange range) {
        Preconditions.checkArgument(range.size() >= requested, "Not enough timestamps provided");
    }

    private static final class MultipleRangeBackedExactTimestampSupplier implements LongSupplier {
        // invariant: the current range is the top of the queue
        private final int requested;
        private final Queue<TimestampRange> ranges;
        private int provided = 0;
        private int providedFromCurrentRange = 0;

        private MultipleRangeBackedExactTimestampSupplier(int requested, Queue<TimestampRange> ranges) {
            this.requested = requested;
            this.ranges = ranges;
        }

        @Override
        public synchronized long getAsLong() {
            Preconditions.checkArgument(provided < requested, "All requested timestamps have been provided");
            TimestampRange currentRange = getCurrentTimestampRange();
            long timestamp = currentRange.getLowerBound() + providedFromCurrentRange;
            providedFromCurrentRange++;
            provided++;
            return timestamp;
        }

        private TimestampRange getCurrentTimestampRange() {
            if (ranges.isEmpty()) {
                throw new SafeIllegalStateException("No more timestamps available");
            }

            TimestampRange top = ranges.peek();
            if (providedFromCurrentRange == top.size()) {
                ranges.poll();
                TimestampRange nextRange =
                        Preconditions.checkArgumentNotNull(ranges.peek(), "No more timestamps available");
                providedFromCurrentRange = 0;
                return nextRange;
            }

            return top;
        }
    }

    private static final class SingleRangeBackedExactTimestampSupplier implements LongSupplier {
        private final TimestampRange range;
        private final int requested;
        private int provided = 0;

        private SingleRangeBackedExactTimestampSupplier(TimestampRange range, int requested) {
            this.range = range;
            this.requested = requested;
        }

        @Override
        public long getAsLong() {
            if (provided >= requested) {
                throw new SafeIllegalStateException("All requested timestamps have been provided");
            }

            long timestamp = range.getLowerBound() + provided;
            provided++;
            return timestamp;
        }
    }
}
