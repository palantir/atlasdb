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

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

// This exists as an abstract class, because the semantics of the atomic table operations are tricky, and novel as
// part of the auto-scaling sweep project. We want to test these against the various DB implementations
public abstract class AbstractDefaultBucketProgressStoreTest {
    private static final Bucket BUCKET_ONE_CONSERVATIVE_SHARD_ZERO =
            Bucket.of(ShardAndStrategy.of(0, SweeperStrategy.CONSERVATIVE), 1);
    private static final Bucket BUCKET_TWO_CONSERVATIVE_SHARD_ZERO =
            Bucket.of(ShardAndStrategy.of(0, SweeperStrategy.CONSERVATIVE), 2);
    private static final Bucket BUCKET_ONE_CONSERVATIVE_SHARD_ONE =
            Bucket.of(ShardAndStrategy.of(1, SweeperStrategy.CONSERVATIVE), 1);
    private static final Bucket BUCKET_ONE_THOROUGH_SHARD_ZERO =
            Bucket.of(ShardAndStrategy.of(0, SweeperStrategy.THOROUGH), 1);
    private static final Bucket BUCKET_ONE_THOROUGH_SHARD_ONE =
            Bucket.of(ShardAndStrategy.of(1, SweeperStrategy.THOROUGH), 1);
    private static final Bucket BUCKET_ONE_NON_SWEEPABLE_SHARD_ZERO =
            Bucket.of(ShardAndStrategy.of(0, SweeperStrategy.NON_SWEEPABLE), 1);

    private static final BucketProgress PROGRESS_ONE_THOUSAND = BucketProgress.createForTimestampProgress(1000L);
    private static final BucketProgress PROGRESS_TWO_THOUSAND_NO_CELLS_SWEPT =
            BucketProgress.createForTimestampProgress(2000L);
    private static final BucketProgress PROGRESS_TWO_THOUSAND_ONE_CELL_SWEPT = BucketProgress.builder()
            .timestampProgress(2000L)
            .cellProgressForNextTimestamp(0L)
            .build();

    private final KeyValueService kvs;
    private final BucketProgressStore store;

    protected AbstractDefaultBucketProgressStoreTest(KvsManager kvsManager) {
        kvs = kvsManager.getDefaultKvs();
        store = DefaultBucketProgressStore.create(kvs);
    }

    @BeforeEach
    public void setUp() {
        Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
        kvs.truncateTable(DefaultBucketProgressStore.TABLE_REF);
    }

    @ParameterizedTest
    @MethodSource("testBuckets")
    public void bucketProgressIsEmptyIfNothingStored(Bucket bucket) {
        assertThat(store.getBucketProgress(bucket)).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("testBuckets")
    public void bucketProgressReturnsStoredValue(Bucket bucket) {
        store.updateBucketProgressToAtLeast(bucket, PROGRESS_ONE_THOUSAND);
        assertThat(store.getBucketProgress(bucket)).contains(PROGRESS_ONE_THOUSAND);
        testBuckets().filter(testBucket -> !testBucket.equals(bucket)).forEach(testBucket -> {
            assertThat(store.getBucketProgress(testBucket)).isEmpty();
        });
    }

    @ParameterizedTest
    @MethodSource("testBuckets")
    public void updateBucketProgressToAtLeastIncreasesExistingProgress(Bucket bucket) {
        store.updateBucketProgressToAtLeast(bucket, PROGRESS_ONE_THOUSAND);
        assertThat(store.getBucketProgress(bucket)).contains(PROGRESS_ONE_THOUSAND);
        store.updateBucketProgressToAtLeast(bucket, PROGRESS_TWO_THOUSAND_NO_CELLS_SWEPT);
        assertThat(store.getBucketProgress(bucket)).contains(PROGRESS_TWO_THOUSAND_NO_CELLS_SWEPT);
        store.updateBucketProgressToAtLeast(bucket, PROGRESS_TWO_THOUSAND_ONE_CELL_SWEPT);
        assertThat(store.getBucketProgress(bucket)).contains(PROGRESS_TWO_THOUSAND_ONE_CELL_SWEPT);
    }

    @ParameterizedTest
    @MethodSource("testBuckets")
    public void updateBucketProgressToAtLeastDoesNotDecreaseExistingProgress(Bucket bucket) {
        store.updateBucketProgressToAtLeast(bucket, PROGRESS_TWO_THOUSAND_ONE_CELL_SWEPT);
        assertThat(store.getBucketProgress(bucket)).contains(PROGRESS_TWO_THOUSAND_ONE_CELL_SWEPT);
        store.updateBucketProgressToAtLeast(bucket, PROGRESS_ONE_THOUSAND);
        assertThat(store.getBucketProgress(bucket)).contains(PROGRESS_TWO_THOUSAND_ONE_CELL_SWEPT);
        store.updateBucketProgressToAtLeast(bucket, PROGRESS_TWO_THOUSAND_NO_CELLS_SWEPT);
        assertThat(store.getBucketProgress(bucket)).contains(PROGRESS_TWO_THOUSAND_ONE_CELL_SWEPT);
    }

    @Test
    public void deleteBucketProgressDoesNotThrowIfBucketProgressNotPresent() {
        assertThatCode(() -> store.deleteBucketProgress(BUCKET_ONE_CONSERVATIVE_SHARD_ZERO))
                .doesNotThrowAnyException();
    }

    @Test
    public void deleteBucketProgressDeletesBucketProgress() {
        store.updateBucketProgressToAtLeast(BUCKET_ONE_CONSERVATIVE_SHARD_ZERO, PROGRESS_ONE_THOUSAND);
        assertThat(store.getBucketProgress(BUCKET_ONE_CONSERVATIVE_SHARD_ZERO)).contains(PROGRESS_ONE_THOUSAND);

        store.deleteBucketProgress(BUCKET_ONE_CONSERVATIVE_SHARD_ZERO);
        assertThat(store.getBucketProgress(BUCKET_ONE_CONSERVATIVE_SHARD_ZERO)).isEmpty();
    }

    private static Stream<Bucket> testBuckets() {
        return Stream.of(
                BUCKET_ONE_CONSERVATIVE_SHARD_ZERO,
                BUCKET_TWO_CONSERVATIVE_SHARD_ZERO,
                BUCKET_ONE_CONSERVATIVE_SHARD_ONE,
                BUCKET_ONE_THOROUGH_SHARD_ZERO,
                BUCKET_ONE_THOROUGH_SHARD_ONE,
                BUCKET_ONE_NON_SWEEPABLE_SHARD_ZERO);
    }
}
