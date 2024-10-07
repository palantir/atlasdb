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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.sweep.asts.bucketingthings.ObjectPersister.LogSafety;
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public final class BucketIdentifierPersisterTest {
    private static final ObjectPersister<Long> PERSISTER =
            ObjectPersister.of(ObjectMappers.newServerSmileMapper(), Long.class, LogSafety.SAFE);

    private static final long BUCKET_IDENTIFIER_ONE = 12321323918230981L;
    private static final long BUCKET_IDENTIFIER_TWO = 1239019283092131L;

    // Be very careful about changing these without a migration.
    private static final byte[] SERIALIZED_BUCKET_IDENTIFIER_ONE =
            BaseEncoding.base64().decode("OikKBSVXRhZxaXAuig==");

    private static final byte[] SERIALIZED_BUCKET_IDENTIFIER_TWO =
            BaseEncoding.base64().decode("OikKBSUIZnBqB1EVhg==");

    @ParameterizedTest
    @MethodSource("bucketIdentifiers")
    public void deserializingBucketIdentifierIsInverseOfSerialization(long bucketIdentifier) {
        byte[] serialized = PERSISTER.trySerialize(bucketIdentifier);
        assertThat(PERSISTER.tryDeserialize(serialized)).isEqualTo(bucketIdentifier);
    }

    @ParameterizedTest
    @MethodSource("bucketIdentifiers")
    public void canDeserializeExistingVersionOfBucketIdentifier(long bucketIdentifier, byte[] serialized) {
        assertThat(PERSISTER.tryDeserialize(serialized)).isEqualTo(bucketIdentifier);
    }

    private static Stream<Arguments> bucketIdentifiers() {
        return Stream.of(
                Arguments.of(BUCKET_IDENTIFIER_ONE, SERIALIZED_BUCKET_IDENTIFIER_ONE),
                Arguments.of(BUCKET_IDENTIFIER_TWO, SERIALIZED_BUCKET_IDENTIFIER_TWO));
    }
}
