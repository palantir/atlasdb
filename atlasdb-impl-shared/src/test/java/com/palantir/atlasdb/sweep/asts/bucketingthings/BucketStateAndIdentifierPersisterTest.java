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
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public final class BucketStateAndIdentifierPersisterTest {
    private static final ObjectPersister<BucketStateAndIdentifier> PERSISTER =
            ObjectPersister.of(ObjectMappers.newServerSmileMapper(), BucketStateAndIdentifier.class, LogSafety.SAFE);

    // We're not enumerating all states here as we test serde in BucketAssignerStateTest
    private static final BucketStateAndIdentifier BUCKET_STATE_AND_IDENTIFIER_ONE =
            ImmutableBucketStateAndIdentifier.builder()
                    .bucketIdentifier(123L)
                    .state(BucketAssignerState.start(1))
                    .build();

    private static final BucketStateAndIdentifier BUCKET_STATE_AND_IDENTIFIER_TWO =
            ImmutableBucketStateAndIdentifier.builder()
                    .bucketIdentifier(456L)
                    .state(BucketAssignerState.immediatelyClosing(123, 541))
                    .build();

    // Be very careful about changing these without a migration.
    private static final byte[] SERIALIZED_BUCKET_STATE_AND_IDENTIFIER_ONE = BaseEncoding.base64()
            .decode("OikKBfqPYnVja2V0SWRlbnRpZmllciQDtoRzdGF0ZfqDdHlwZURzdGFydJZzdGFydFRpbWVzdGFtcEluY2x1c2l2ZcL7+w==");

    private static final byte[] SERIALIZED_BUCKET_STATE_AND_IDENTIFIER_TWO = BaseEncoding.base64()
            .decode(
                    "OikKBfqPYnVja2V0SWRlbnRpZmllciQOkIRzdGF0ZfqDdHlwZVFpbW1lZGlhdGVseUNsb3NpbmeWc3RhcnRUaW1lc3RhbXBJbmNsdXNpdmUkA7aUZW5kVGltZXN0YW1wRXhjbHVzaXZlJBC6+/s=");

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

    private static Stream<Arguments> bucketStateAndIdentifiers() {
        return Stream.of(
                Arguments.of(BUCKET_STATE_AND_IDENTIFIER_ONE, SERIALIZED_BUCKET_STATE_AND_IDENTIFIER_ONE),
                Arguments.of(BUCKET_STATE_AND_IDENTIFIER_TWO, SERIALIZED_BUCKET_STATE_AND_IDENTIFIER_TWO));
    }
}
