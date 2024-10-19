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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.logsafe.SafeArg;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Stream;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BucketAssignerStateTest {
    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newSmileServerObjectMapper();

    // Think _very_ carefully about changing this without a migration.
    private static final byte[] START_SERIALIZED =
            BaseEncoding.base64().decode("OikKBfqDdHlwZURzdGFydJZzdGFydFRpbWVzdGFtcEluY2x1c2l2ZSQTCTSA+w==");
    private static final byte[] OPENING_SERIALIZED =
            BaseEncoding.base64().decode("OikKBfqDdHlwZUZvcGVuaW5nlnN0YXJ0VGltZXN0YW1wSW5jbHVzaXZlJCYSaID7");
    private static final byte[] WAITING_UNTIL_CLOSED_SERIALIZED = BaseEncoding.base64()
            .decode("OikKBfqDdHlwZVR3YWl0aW5nVW50aWxDbG9zZWFibGWWc3RhcnRUaW1lc3RhbXBJbmNsdXNpdmUkORwcgPs=");
    private static final byte[] CLOSING_FROM_OPEN_SERIALIZED = BaseEncoding.base64()
            .decode(
                    "OikKBfqDdHlwZU5jbG9zaW5nRnJvbU9wZW6Wc3RhcnRUaW1lc3RhbXBJbmNsdXNpdmUkTCVQgJRlbmRUaW1lc3RhbXBFeGNsdXNpdmUkXy8EgPs=");
    private static final byte[] IMMEDIATELY_CLOSING_SERIALIZED = BaseEncoding.base64()
            .decode(
                    "OikKBfqDdHlwZVFpbW1lZGlhdGVseUNsb3NpbmeWc3RhcnRUaW1lc3RhbXBJbmNsdXNpdmUkcjg4gJRlbmRUaW1lc3RhbXBFeGNsdXNpdmUkAQVBbID7");

    @ParameterizedTest(name = "{0}")
    @MethodSource("bucketAssignerStates")
    public void canRoundTripSerdeState(BucketAssignerState state, byte[] _unused) throws IOException {
        byte[] json = OBJECT_MAPPER.writeValueAsBytes(state);
        BucketAssignerState deserialized = OBJECT_MAPPER.readValue(json, BucketAssignerState.class);
        assertThat(deserialized).isEqualTo(state);
        // Adding instance of check to be _really_ sure, since a mistake here would be very painful.
        assertThat(deserialized).isInstanceOf(state.getClass());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("bucketAssignerStates")
    public void canDeserializeExistingVersion(BucketAssignerState bucketProgress, byte[] serializedForm)
            throws IOException {
        assertThat(OBJECT_MAPPER.readValue(serializedForm, BucketAssignerState.class))
                .isEqualTo(bucketProgress);
    }

    @ParameterizedTest
    @MethodSource("badTimestamps")
    public void failsToConstructStartingIfStartTimestampIsNotOnCoarsePartitionBoundary(long badStartTimestamp) {
        assertStateWithOnlyStartTimestampFieldThrowsValidationError(
                () -> BucketAssignerState.start(badStartTimestamp), badStartTimestamp);
    }

    @ParameterizedTest
    @MethodSource("badTimestamps")
    public void failsToConstructOpeningIfStartTimestampIsNotOnCoarsePartitionBoundary(long badStartTimestamp) {
        assertStateWithOnlyStartTimestampFieldThrowsValidationError(
                () -> BucketAssignerState.opening(badStartTimestamp), badStartTimestamp);
    }

    @ParameterizedTest
    @MethodSource("badTimestamps")
    public void failsToConstructWaitingUntilCloseableIfStartTimestampIsNotOnCoarsePartitionBoundary(
            long badStartTimestamp) {
        assertStateWithOnlyStartTimestampFieldThrowsValidationError(
                () -> BucketAssignerState.waitingUntilCloseable(badStartTimestamp), badStartTimestamp);
    }

    @ParameterizedTest
    @MethodSource("badStartAndEndTimestamps")
    public void failsToConstructClosingFromOpenIfStartTimestampIsNotOnCoarsePartitionBoundary(
            long startTimestamp, long endTimestamp) {
        assertStateWithStartAndEndTimestampFieldThrowsValidationError(
                () -> BucketAssignerState.closingFromOpen(startTimestamp, endTimestamp), startTimestamp, endTimestamp);
    }

    @ParameterizedTest
    @MethodSource("badStartAndEndTimestamps")
    public void failsToConstructImmediatelyClosingIfStartTimestampIsNotOnCoarsePartitionBoundary(
            long startTimestamp, long endTimestamp) {
        assertStateWithStartAndEndTimestampFieldThrowsValidationError(
                () -> BucketAssignerState.immediatelyClosing(startTimestamp, endTimestamp),
                startTimestamp,
                endTimestamp);
    }

    private static Stream<Arguments> bucketAssignerStates() {
        return Stream.of(
                Arguments.of(BucketAssignerState.start(coarsePartitionTimestamp(1)), START_SERIALIZED),
                Arguments.of(BucketAssignerState.opening(coarsePartitionTimestamp(2)), OPENING_SERIALIZED),
                Arguments.of(
                        BucketAssignerState.waitingUntilCloseable(coarsePartitionTimestamp(3)),
                        WAITING_UNTIL_CLOSED_SERIALIZED),
                Arguments.of(
                        BucketAssignerState.closingFromOpen(coarsePartitionTimestamp(4), coarsePartitionTimestamp(5)),
                        CLOSING_FROM_OPEN_SERIALIZED),
                Arguments.of(
                        BucketAssignerState.immediatelyClosing(
                                coarsePartitionTimestamp(6), coarsePartitionTimestamp(7)),
                        IMMEDIATELY_CLOSING_SERIALIZED));
    }

    private static Stream<Long> badTimestamps() {
        return Stream.of(1L, coarsePartitionTimestamp(1) + 2, Long.MAX_VALUE);
    }

    private static Stream<Arguments> badStartAndEndTimestamps() {
        Set<Long> validTimestamps = Set.of(coarsePartitionTimestamp(1), coarsePartitionTimestamp(22));

        Stream<Arguments> badStartTimestamps =
                badTimestamps().flatMap(start -> validTimestamps.stream().map(end -> Arguments.of(start, end)));

        Stream<Arguments> badEndTimestamps =
                badTimestamps().flatMap(end -> validTimestamps.stream().map(start -> Arguments.of(start, end)));

        Stream<Arguments> badBothTimestamps =
                badTimestamps().flatMap(start -> badTimestamps().flatMap(end -> Stream.of(Arguments.of(start, end))));

        return Stream.concat(badStartTimestamps, Stream.concat(badEndTimestamps, badBothTimestamps));
    }

    private static long coarsePartitionTimestamp(long coarsePartition) {
        return SweepQueueUtils.minTsForCoarsePartition(coarsePartition);
    }

    private void assertStateWithOnlyStartTimestampFieldThrowsValidationError(
            ThrowingCallable callable, long badStartTimestamp) {
        assertThatLoggableExceptionThrownBy(callable)
                .hasLogMessage("Start timestamp must be on a coarse partition boundary")
                .hasExactlyArgs(SafeArg.of("startTimestampInclusive", badStartTimestamp));
    }

    private void assertStateWithStartAndEndTimestampFieldThrowsValidationError(
            ThrowingCallable callable, long startTimestamp, long endTimestamp) {
        assertThatLoggableExceptionThrownBy(callable)
                .hasLogMessage("Start and end timestamp must be on a coarse partition boundary")
                .hasExactlyArgs(
                        SafeArg.of("startTimestampInclusive", startTimestamp),
                        SafeArg.of("endTimestampExclusive", endTimestamp));
    }
}
