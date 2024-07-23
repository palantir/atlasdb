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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.sweep.asts.SweepStateCoordinator.SweepableBucket;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.conjure.java.serialization.ObjectMappers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

// Much of the tests of basic behaviour on this class are covered by the abstract test, because we are concerned
// about how we actually work with the underlying behaviour of the key-value-services (and, some of that functionality
// was added as part of this work).
@ExtendWith(MockitoExtension.class)
public class DefaultBucketProgressStoreTest {
    private static final RuntimeException GENERIC_RUNTIME_EXCEPTION = new RuntimeException("oops");
    private static final SweepableBucket DEFAULT_BUCKET = SweepableBucket.of(ShardAndStrategy.thorough(0), 1L);
    private static final Cell DEFAULT_BUCKET_CELL = DefaultBucketKeySerializer.INSTANCE.bucketToCell(DEFAULT_BUCKET);

    private static final BucketProgressSerializer BUCKET_PROGRESS_SERIALIZER =
            BucketProgressSerializer.create(ObjectMappers.newSmileServerObjectMapper());

    @Mock
    private KeyValueService keyValueService;

    private BucketProgressStore bucketProgressStore;

    @BeforeEach
    public void setUp() {
        this.bucketProgressStore = new DefaultBucketProgressStore(keyValueService, BUCKET_PROGRESS_SERIALIZER);
    }

    @Test
    public void updateBucketProgressToAtLeastRetriesOnFailure() {
        doThrow(GENERIC_RUNTIME_EXCEPTION).doNothing().when(keyValueService).checkAndSet(any());
        assertThatCode(() -> bucketProgressStore.updateBucketProgressToAtLeast(
                DEFAULT_BUCKET, BucketProgress.createForTimestampOffset(1000L)))
                .doesNotThrowAnyException();
        verify(keyValueService, times(2)).checkAndSet(any());
    }

    @Test
    public void updateBucketProgressToAtLeastDoesNotRetryIndefinitely() {
        doThrow(GENERIC_RUNTIME_EXCEPTION).when(keyValueService).checkAndSet(any());
        assertThatThrownBy(() -> bucketProgressStore.updateBucketProgressToAtLeast(
                DEFAULT_BUCKET, BucketProgress.createForTimestampOffset(1000L)))
                .isEqualTo(GENERIC_RUNTIME_EXCEPTION);
        verify(keyValueService, times(10)).checkAndSet(any());
    }

    @Test
    public void updateBucketProgressToAtLeastDoesNotWriteIfInDatabaseProgressIsHigher() {
        when(keyValueService.get(any(), anyMap()))
                .thenReturn(ImmutableMap.of(
                        DEFAULT_BUCKET_CELL,
                        Value.create(
                                BUCKET_PROGRESS_SERIALIZER.serializeProgress(
                                        BucketProgress.createForTimestampOffset(200L)),
                                AtlasDbConstants.TRANSACTION_TS)));

        BucketProgress bucketProgress = BucketProgress.createForTimestampOffset(100L);
        assertThatCode(() -> bucketProgressStore.updateBucketProgressToAtLeast(DEFAULT_BUCKET, bucketProgress))
                .doesNotThrowAnyException();
        verify(keyValueService, never()).checkAndSet(any());
    }

    @Test
    public void updateBucketProgressToAtLeastDoesNotWriteIfInDatabaseProgressIsEqual() {
        when(keyValueService.get(any(), anyMap()))
                .thenReturn(ImmutableMap.of(
                        DEFAULT_BUCKET_CELL,
                        Value.create(
                                BUCKET_PROGRESS_SERIALIZER.serializeProgress(
                                        BucketProgress.createForTimestampOffset(100L)),
                                AtlasDbConstants.TRANSACTION_TS)));

        BucketProgress bucketProgress = BucketProgress.createForTimestampOffset(100L);
        assertThatCode(() -> bucketProgressStore.updateBucketProgressToAtLeast(DEFAULT_BUCKET, bucketProgress))
                .doesNotThrowAnyException();
        verify(keyValueService, never()).checkAndSet(any());
    }

    @Test
    public void updateBucketProgressToAtLeastRetriesWithNewExpectationsIfReceivingCheckAndSetExceptions() {
        BucketProgress bucketProgress = BucketProgress.createForTimestampOffset(100L);

        byte[] serializedBucketProgress = BUCKET_PROGRESS_SERIALIZER.serializeProgress(bucketProgress);
        byte[] serializedIntermediateProgressOne =
                BUCKET_PROGRESS_SERIALIZER.serializeProgress(BucketProgress.createForTimestampOffset(10L));
        byte[] serializedIntermediateProgressTwo =
                BUCKET_PROGRESS_SERIALIZER.serializeProgress(BucketProgress.createForTimestampOffset(50L));
        when(keyValueService.get(any(), anyMap()))
                .thenReturn(
                        ImmutableMap.of(),
                        ImmutableMap.of(
                                DEFAULT_BUCKET_CELL,
                                Value.create(serializedIntermediateProgressOne, AtlasDbConstants.TRANSACTION_TS)),
                        ImmutableMap.of(
                                DEFAULT_BUCKET_CELL,
                                Value.create(serializedIntermediateProgressTwo, AtlasDbConstants.TRANSACTION_TS)));

        doThrow(createCheckAndSetExceptionForDefaultBucket(
                bucketProgress, BucketProgress.createForTimestampOffset(30L)))
                .doThrow(createCheckAndSetExceptionForDefaultBucket(
                        bucketProgress, BucketProgress.createForTimestampOffset(70L)))
                .doNothing()
                .when(keyValueService)
                .checkAndSet(any());

        assertThatCode(() -> bucketProgressStore.updateBucketProgressToAtLeast(DEFAULT_BUCKET, bucketProgress))
                .doesNotThrowAnyException();

        ArgumentCaptor<CheckAndSetRequest> captor = ArgumentCaptor.forClass(CheckAndSetRequest.class);
        verify(keyValueService, times(3)).checkAndSet(captor.capture());
        assertThat(captor.getAllValues())
                .containsExactly(
                        CheckAndSetRequest.newCell(
                                DefaultBucketProgressStore.TABLE_REF, DEFAULT_BUCKET_CELL, serializedBucketProgress),
                        CheckAndSetRequest.singleCell(
                                DefaultBucketProgressStore.TABLE_REF,
                                DEFAULT_BUCKET_CELL,
                                serializedIntermediateProgressOne,
                                serializedBucketProgress),
                        CheckAndSetRequest.singleCell(
                                DefaultBucketProgressStore.TABLE_REF,
                                DEFAULT_BUCKET_CELL,
                                serializedIntermediateProgressTwo,
                                serializedBucketProgress));
    }

    private static CheckAndSetException createCheckAndSetExceptionForDefaultBucket(
            BucketProgress expectedProgress, BucketProgress progressInDatabase) {
        return new CheckAndSetException(
                DEFAULT_BUCKET_CELL,
                DefaultBucketProgressStore.TABLE_REF,
                BUCKET_PROGRESS_SERIALIZER.serializeProgress(expectedProgress),
                ImmutableList.of(BUCKET_PROGRESS_SERIALIZER.serializeProgress(progressInDatabase)));
    }
}
