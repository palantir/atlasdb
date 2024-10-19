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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import org.immutables.value.Value;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = ImmutableStart.class, name = BucketAssignerState.Start.TYPE),
    @JsonSubTypes.Type(value = ImmutableOpening.class, name = BucketAssignerState.Opening.TYPE),
    @JsonSubTypes.Type(
            value = ImmutableWaitingUntilCloseable.class,
            name = BucketAssignerState.WaitingUntilCloseable.TYPE),
    @JsonSubTypes.Type(value = ImmutableClosingFromOpen.class, name = BucketAssignerState.ClosingFromOpen.TYPE),
    @JsonSubTypes.Type(value = ImmutableImmediatelyClosing.class, name = BucketAssignerState.ImmediatelyClosing.TYPE),
})
public interface BucketAssignerState {
    <T> T accept(BucketAssignerState.Visitor<T> visitor);

    static BucketAssignerState.Start start(long startTimestampInclusive) {
        return ImmutableStart.of(startTimestampInclusive);
    }

    static BucketAssignerState.Opening opening(long startTimestampInclusive) {
        return ImmutableOpening.of(startTimestampInclusive);
    }

    static BucketAssignerState.WaitingUntilCloseable waitingUntilCloseable(long startTimestampInclusive) {
        return ImmutableWaitingUntilCloseable.of(startTimestampInclusive);
    }

    static BucketAssignerState.ClosingFromOpen closingFromOpen(
            long startTimestampInclusive, long endTimestampExclusive) {
        return ImmutableClosingFromOpen.of(startTimestampInclusive, endTimestampExclusive);
    }

    static BucketAssignerState.ImmediatelyClosing immediatelyClosing(
            long startTimestampInclusive, long endTimestampExclusive) {
        return ImmutableImmediatelyClosing.of(startTimestampInclusive, endTimestampExclusive);
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableStart.class)
    @JsonDeserialize(as = ImmutableStart.class)
    interface Start extends BucketAssignerState {
        String TYPE = "start";

        @Value.Parameter
        long startTimestampInclusive();

        @Override
        default <T> T accept(BucketAssignerState.Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Value.Check
        default void check() {
            Preconditions.checkState(
                    timestampIsOnCoarsePartitionBoundary(startTimestampInclusive()),
                    "Start timestamp must be on a coarse partition boundary",
                    SafeArg.of("startTimestampInclusive", startTimestampInclusive()));
        }
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableOpening.class)
    @JsonDeserialize(as = ImmutableOpening.class)
    interface Opening extends BucketAssignerState {
        String TYPE = "opening";

        @Value.Parameter
        long startTimestampInclusive();

        @Override
        default <T> T accept(BucketAssignerState.Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Value.Check
        default void check() {
            Preconditions.checkState(
                    timestampIsOnCoarsePartitionBoundary(startTimestampInclusive()),
                    "Start timestamp must be on a coarse partition boundary",
                    SafeArg.of("startTimestampInclusive", startTimestampInclusive()));
        }
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableWaitingUntilCloseable.class)
    @JsonDeserialize(as = ImmutableWaitingUntilCloseable.class)
    interface WaitingUntilCloseable extends BucketAssignerState {
        String TYPE = "waitingUntilCloseable";

        @Value.Parameter
        long startTimestampInclusive();

        @Override
        default <T> T accept(BucketAssignerState.Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Value.Check
        default void check() {
            Preconditions.checkState(
                    timestampIsOnCoarsePartitionBoundary(startTimestampInclusive()),
                    "Start timestamp must be on a coarse partition boundary",
                    SafeArg.of("startTimestampInclusive", startTimestampInclusive()));
        }
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableClosingFromOpen.class)
    @JsonDeserialize(as = ImmutableClosingFromOpen.class)
    interface ClosingFromOpen extends BucketAssignerState {
        String TYPE = "closingFromOpen";

        @Value.Parameter
        long startTimestampInclusive();

        @Value.Parameter
        long endTimestampExclusive();

        @Override
        default <T> T accept(BucketAssignerState.Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Value.Check
        default void check() {
            Preconditions.checkState(
                    timestampIsOnCoarsePartitionBoundary(startTimestampInclusive())
                            && timestampIsOnCoarsePartitionBoundary(endTimestampExclusive()),
                    "Start and end timestamp must be on a coarse partition boundary",
                    SafeArg.of("startTimestampInclusive", startTimestampInclusive()),
                    SafeArg.of("endTimestampExclusive", endTimestampExclusive()));
        }
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableImmediatelyClosing.class)
    @JsonDeserialize(as = ImmutableImmediatelyClosing.class)
    interface ImmediatelyClosing extends BucketAssignerState {
        String TYPE = "immediatelyClosing";

        @Value.Parameter
        long startTimestampInclusive();

        @Value.Parameter
        long endTimestampExclusive();

        @Override
        default <T> T accept(BucketAssignerState.Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Value.Check
        default void check() {
            Preconditions.checkState(
                    timestampIsOnCoarsePartitionBoundary(startTimestampInclusive())
                            && timestampIsOnCoarsePartitionBoundary(endTimestampExclusive()),
                    "Start and end timestamp must be on a coarse partition boundary",
                    SafeArg.of("startTimestampInclusive", startTimestampInclusive()),
                    SafeArg.of("endTimestampExclusive", endTimestampExclusive()));
        }
    }

    interface Visitor<T> {
        T visit(BucketAssignerState.Start start);

        T visit(BucketAssignerState.Opening opening);

        T visit(BucketAssignerState.WaitingUntilCloseable waitingUntilCloseable);

        T visit(BucketAssignerState.ClosingFromOpen closingFromOpen);

        T visit(BucketAssignerState.ImmediatelyClosing immediatelyClosing);
    }

    private static boolean timestampIsOnCoarsePartitionBoundary(long timestamp) {
        return timestamp % SweepQueueUtils.TS_COARSE_GRANULARITY == 0;
    }
}
