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

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.sweep.asts.ImmutableSweepableBucket;
import com.palantir.atlasdb.sweep.asts.SweepStateCoordinator.SweepableBucket;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class AbstractBucketProgressStoreTest {
    private static final SweepableBucket SWEEPABLE_BUCKET_ONE_CONSERVATIVE_SHARD_ZERO =
            ImmutableSweepableBucket.of(ShardAndStrategy.of(0, SweeperStrategy.CONSERVATIVE), 1);
    private static final SweepableBucket SWEEPABLE_BUCKET_TWO_CONSERVATIVE_SHARD_ZERO =
            ImmutableSweepableBucket.of(ShardAndStrategy.of(0, SweeperStrategy.CONSERVATIVE), 2);
    private static final SweepableBucket SWEEPABLE_BUCKET_ONE_CONSERVATIVE_SHARD_ONE =
            ImmutableSweepableBucket.of(ShardAndStrategy.of(1, SweeperStrategy.CONSERVATIVE), 1);
    private static final SweepableBucket SWEEPABLE_BUCKET_ONE_THOROUGH_SHARD_ZERO =
            ImmutableSweepableBucket.of(ShardAndStrategy.of(0, SweeperStrategy.THOROUGH), 1);
    private static final SweepableBucket SWEEPABLE_BUCKET_ONE_NON_SWEEPABLE_SHARD_ZERO =
            ImmutableSweepableBucket.of(ShardAndStrategy.of(0, SweeperStrategy.NON_SWEEPABLE), 1);

    private static final BucketProgress PROGRESS_ONE_THOUSAND =
            ImmutableBucketProgress.builder().timestampOffset(1000L).build();
    private static final BucketProgress PROGRESS_TWO_THOUSAND =
            ImmutableBucketProgress.builder().timestampOffset(2000L).build();

    private KeyValueService kvs;
    private BucketProgressStore store;

    protected AbstractBucketProgressStoreTest(KvsManager kvsManager) {
        this.kvs = kvsManager.getDefaultKvs();
    }

    @BeforeEach
    public void setup() {
        store = new DefaultBucketProgressStore(kvs);
        Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
    }

    @ParameterizedTest
    @MethodSource("testBuckets")
    public void bucketProgressIsEmptyIfNothingStored(SweepableBucket bucket) {
        assertThat(store.getBucketProgress(bucket)).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("testBuckets")
    public void bucketProgressReturnsStoredValue(SweepableBucket bucket) {
        store.updateBucketProgressToAtLeast(bucket, PROGRESS_ONE_THOUSAND);
        assertThat(store.getBucketProgress(bucket)).contains(PROGRESS_ONE_THOUSAND);
        testBuckets().filter(testBucket -> !testBucket.equals(bucket)).forEach(testBucket -> {
            assertThat(store.getBucketProgress(testBucket)).isEmpty();
        });
    }

    @ParameterizedTest
    @MethodSource("testBuckets")
    public void updateBucketProgressToAtLeastIncreasesExistingProgress(SweepableBucket bucket) {
        store.updateBucketProgressToAtLeast(bucket, PROGRESS_ONE_THOUSAND);
        assertThat(store.getBucketProgress(bucket)).contains(PROGRESS_ONE_THOUSAND);
        store.updateBucketProgressToAtLeast(bucket, PROGRESS_TWO_THOUSAND);
        assertThat(store.getBucketProgress(bucket)).contains(PROGRESS_TWO_THOUSAND);
    }

    @ParameterizedTest
    @MethodSource("testBuckets")
    public void updateBucketProgressToAtLeastDoesNotDecreaseExistingProgress(SweepableBucket bucket) {
        store.updateBucketProgressToAtLeast(bucket, PROGRESS_TWO_THOUSAND);
        assertThat(store.getBucketProgress(bucket)).contains(PROGRESS_TWO_THOUSAND);
        store.updateBucketProgressToAtLeast(bucket, PROGRESS_ONE_THOUSAND);
        assertThat(store.getBucketProgress(bucket)).contains(PROGRESS_TWO_THOUSAND);
    }

    @ParameterizedTest
    @MethodSource("testBuckets")
    public void progressReturnsEmptyAfterMarkingComplete(SweepableBucket bucket) {
        store.updateBucketProgressToAtLeast(bucket, PROGRESS_ONE_THOUSAND);
        assertThat(store.getBucketProgress(bucket)).contains(PROGRESS_ONE_THOUSAND);
        store.markBucketComplete(bucket);
        assertThat(store.getBucketProgress(bucket)).isEmpty();
    }

    private static Stream<SweepableBucket> testBuckets() {
        return Stream.of(
                SWEEPABLE_BUCKET_ONE_CONSERVATIVE_SHARD_ZERO,
                SWEEPABLE_BUCKET_TWO_CONSERVATIVE_SHARD_ZERO,
                SWEEPABLE_BUCKET_ONE_CONSERVATIVE_SHARD_ONE,
                SWEEPABLE_BUCKET_ONE_THOROUGH_SHARD_ZERO,
                SWEEPABLE_BUCKET_ONE_NON_SWEEPABLE_SHARD_ZERO);
    }
}
