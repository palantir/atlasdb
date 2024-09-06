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

package com.palantir.atlasdb.sweep.asts.progress;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BucketProgressPersisterTest {
    private static final long TIMESTAMP_OFFSET_1 = 100L;
    private static final long TIMESTAMP_OFFSET_2 = 200L;
    private static final long CELL_OFFSET_1 = 1000000L;
    private static final long CELL_OFFSET_2 = 2000000L;

    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newSmileServerObjectMapper();
    private static final BucketProgress BUCKET_PROGRESS_1 = BucketProgress.builder()
            .timestampProgress(TIMESTAMP_OFFSET_1)
            .cellProgressForNextTimestamp(CELL_OFFSET_1)
            .build();
    private static final BucketProgress BUCKET_PROGRESS_2 = BucketProgress.builder()
            .timestampProgress(TIMESTAMP_OFFSET_2)
            .cellProgressForNextTimestamp(CELL_OFFSET_2)
            .build();

    // Think very carefully about changing these without a migration.
    private static final byte[] SERIALIZED_BUCKET_PROGRESS_1 = BaseEncoding.base64()
            .decode("OikKBfqQdGltZXN0YW1wUHJvZ3Jlc3MkA4ibY2VsbFByb2dyZXNzRm9yTmV4dFRpbWVzdGFtcCQBdBKA+w==");
    private static final byte[] SERIALIZED_BUCKET_PROGRESS_2 = BaseEncoding.base64()
            .decode("OikKBfqQdGltZXN0YW1wUHJvZ3Jlc3MkBpCbY2VsbFByb2dyZXNzRm9yTmV4dFRpbWVzdGFtcCQDaCSA+w==");

    private final BucketProgressPersister bucketProgressPersister = BucketProgressPersister.create(OBJECT_MAPPER);

    @ParameterizedTest(name = "{0}")
    @MethodSource("bucketProgresses")
    public void deserializationIsInverseOfSerialization(BucketProgress bucketProgress, byte[] _unused) {
        assertThat(bucketProgressPersister.deserializeProgress(
                        bucketProgressPersister.serializeProgress(bucketProgress)))
                .isEqualTo(bucketProgress);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("bucketProgresses")
    public void canDeserializeExistingVersionOfProgress(BucketProgress bucketProgress, byte[] serializedForm) {
        assertThat(bucketProgressPersister.deserializeProgress(serializedForm)).isEqualTo(bucketProgress);
    }

    private static Stream<Arguments> bucketProgresses() {
        return Stream.of(
                Arguments.of(BUCKET_PROGRESS_1, SERIALIZED_BUCKET_PROGRESS_1),
                Arguments.of(BUCKET_PROGRESS_2, SERIALIZED_BUCKET_PROGRESS_2));
    }
}
