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

import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public final class BucketProgressSerializationTest {
    @ParameterizedTest
    @MethodSource("testContexts")
    public void serializationMatchesPersistedForm(BucketProgressTestContext testContext) {
        assertThat(BaseEncoding.base64().encode(BucketProgressSerialization.serialize(testContext.bucketProgress())))
                .isEqualTo(testContext.base64EncodedPersistedForm());
    }

    @ParameterizedTest
    @MethodSource("testContexts")
    public void deserializationOfPersistedFormMatches(BucketProgressTestContext testContext) {
        assertThat(BucketProgressSerialization.deserialize(
                        BaseEncoding.base64().decode(testContext.base64EncodedPersistedForm())))
                .isEqualTo(testContext.bucketProgress());
    }

    @ParameterizedTest
    @MethodSource("testContexts")
    public void deserializationIsInverseOfSerialization(BucketProgressTestContext testContext) {
        assertThat(BucketProgressSerialization.deserialize(
                        BucketProgressSerialization.serialize(testContext.bucketProgress())))
                .isEqualTo(testContext.bucketProgress());
    }

    public static Stream<BucketProgressTestContext> testContexts() {
        return Stream.of(
                BucketProgressTestContext.builder()
                        .bucketProgress(
                                BucketProgress.builder().timestampOffset(0L).build())
                        .base64EncodedPersistedForm("eyJ0aW1lc3RhbXBPZmZzZXQiOjB9")
                        .build(),
                BucketProgressTestContext.builder()
                        .bucketProgress(
                                BucketProgress.builder().timestampOffset(12345L).build())
                        .base64EncodedPersistedForm("eyJ0aW1lc3RhbXBPZmZzZXQiOjEyMzQ1fQ==")
                        .build(),
                BucketProgressTestContext.builder()
                        .bucketProgress(BucketProgress.builder()
                                .timestampOffset(SweepQueueUtils.TS_FINE_GRANULARITY - 1)
                                .build())
                        .base64EncodedPersistedForm("eyJ0aW1lc3RhbXBPZmZzZXQiOjQ5OTk5fQ==")
                        .build(),
                // There is no guarantee that in the long term this aligns along fine partitions.
                BucketProgressTestContext.builder()
                        .bucketProgress(BucketProgress.builder()
                                .timestampOffset(Long.MAX_VALUE)
                                .build())
                        .base64EncodedPersistedForm("eyJ0aW1lc3RhbXBPZmZzZXQiOjkyMjMzNzIwMzY4NTQ3NzU4MDd9")
                        .build());
    }

    @Value.Immutable
    interface BucketProgressTestContext {
        BucketProgress bucketProgress();

        String base64EncodedPersistedForm();

        static ImmutableBucketProgressTestContext.Builder builder() {
            return ImmutableBucketProgressTestContext.builder();
        }
    }
}
