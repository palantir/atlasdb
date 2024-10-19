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

import static com.palantir.logsafe.testing.Assertions.assertThat;

import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.sweep.asts.bucketingthings.ObjectPersister.LogSafety;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public final class BucketStateAndIdentifierPersisterTest {
    private static final ObjectPersister<BucketStateAndIdentifier> PERSISTER =
            ObjectPersister.of(BucketStateAndIdentifier.class, LogSafety.SAFE);

    // We're not enumerating all states here as we test serde in BucketAssignerStateTest
    private static final BucketStateAndIdentifier BUCKET_STATE_AND_IDENTIFIER_ONE =
            ImmutableBucketStateAndIdentifier.builder()
                    .bucketIdentifier(123L)
                    .state(BucketAssignerState.start(SweepQueueUtils.TS_COARSE_GRANULARITY * 3))
                    .build();

    private static final BucketStateAndIdentifier BUCKET_STATE_AND_IDENTIFIER_TWO =
            ImmutableBucketStateAndIdentifier.builder()
                    .bucketIdentifier(456L)
                    .state(BucketAssignerState.immediatelyClosing(
                            SweepQueueUtils.TS_COARSE_GRANULARITY * 5, SweepQueueUtils.TS_COARSE_GRANULARITY * 8))
                    .build();

    // Be very careful about changing these without a migration.
    private static final byte[] SERIALIZED_BUCKET_STATE_AND_IDENTIFIER_ONE = BaseEncoding.base64()
            .decode(
                    "OikKBfqPYnVja2V0SWRlbnRpZmllciQDtoRzdGF0ZfqDdHlwZURzdGFydJZzdGFydFRpbWVzdGFtcEluY2x1c2l2ZSQ5HByA+/s=");

    private static final byte[] SERIALIZED_BUCKET_STATE_AND_IDENTIFIER_TWO = BaseEncoding.base64()
            .decode(
                    "OikKBfqPYnVja2V0SWRlbnRpZmllciQOkIRzdGF0ZfqDdHlwZVFpbW1lZGlhdGVseUNsb3NpbmeUZW5kVGltZXN0YW1wRXhjbHVzaXZlJAEYSyCAlnN0YXJ0VGltZXN0YW1wSW5jbHVzaXZlJF8vBID7+w==");

    @ParameterizedTest
    @MethodSource("bucketStateAndIdentifiers")
    public void deserializingBucketStateAndIdentifierIsInverseOfSerialization(
            BucketStateAndIdentifier bucketStateAndIdentifier) {
        byte[] serialized = PERSISTER.trySerialize(bucketStateAndIdentifier);
        assertThat(PERSISTER.tryDeserialize(serialized)).isEqualTo(bucketStateAndIdentifier);
    }

    @ParameterizedTest
    @MethodSource("bucketStateAndIdentifiers")
    public void canDeserializeExistingVersionOfBucketStateAndIdentifier(
            BucketStateAndIdentifier bucketStateAndIdentifier, byte[] serialized) {
        assertThat(PERSISTER.tryDeserialize(serialized)).isEqualTo(bucketStateAndIdentifier);
    }

    @ParameterizedTest
    @MethodSource("bucketStateAndIdentifiers")
    public void serializedBucketMatchesExistingVersion(
            BucketStateAndIdentifier bucketStateAndIdentifier, byte[] serialized) {
        assertThat(serialized).isEqualTo(PERSISTER.trySerialize(bucketStateAndIdentifier));
    }

    private static Stream<Arguments> bucketStateAndIdentifiers() {
        return Stream.of(
                Arguments.of(BUCKET_STATE_AND_IDENTIFIER_ONE, SERIALIZED_BUCKET_STATE_AND_IDENTIFIER_ONE),
                Arguments.of(BUCKET_STATE_AND_IDENTIFIER_TWO, SERIALIZED_BUCKET_STATE_AND_IDENTIFIER_TWO));
    }
}
