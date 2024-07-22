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

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.sweep.asts.SweepStateCoordinator.SweepableBucket;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public final class DefaultBucketKeySerializerTest {
    private static final ShardAndStrategy SHARD_ZERO_CONSERVATIVE = ShardAndStrategy.conservative(0);
    private static final ShardAndStrategy SHARD_ZERO_THOROUGH = ShardAndStrategy.thorough(0);
    private static final ShardAndStrategy SHARD_ONE_CONSERVATIVE = ShardAndStrategy.conservative(1);
    private static final ShardAndStrategy SHARD_ONE_THOROUGH = ShardAndStrategy.thorough(1);
    private static final ShardAndStrategy NON_SWEEPABLE = ShardAndStrategy.nonSweepable();

    // Think extremely carefully about changing this without a migration.
    private static final Map<ShardAndStrategy, Cell> GOLDEN_CELLS = ImmutableMap.of(
            SHARD_ZERO_CONSERVATIVE,
                    Cell.create(PtBytes.decodeHexString("bd3fa0ff210b9da5000001"), PtBytes.decodeHexString("70")),
            SHARD_ZERO_THOROUGH,
                    Cell.create(PtBytes.decodeHexString("f9d54dd1bf713748000000"), PtBytes.decodeHexString("70")),
            SHARD_ONE_CONSERVATIVE,
                    Cell.create(PtBytes.decodeHexString("ff36dc4ac2339d2b010001"), PtBytes.decodeHexString("70")),
            SHARD_ONE_THOROUGH,
                    Cell.create(PtBytes.decodeHexString("a24fa4b6616b22c0010000"), PtBytes.decodeHexString("70")),
            NON_SWEEPABLE,
                    Cell.create(PtBytes.decodeHexString("92635d16672ad89f000002"), PtBytes.decodeHexString("70")));

    @ParameterizedTest
    @MethodSource("testShardsAndStrategies")
    public void sameShardAndStrategyDifferentBucketsMapToDifferentCells(ShardAndStrategy shardAndStrategy) {
        assertBucketsMapToDifferentCells(
                SweepableBucket.of(shardAndStrategy, 0),
                SweepableBucket.of(shardAndStrategy, 1),
                SweepableBucket.of(shardAndStrategy, 2),
                SweepableBucket.of(shardAndStrategy, 3141592));
    }

    @Test
    public void differentShardsAndStrategiesSameBucketMapToDifferentCells() {
        SweepableBucket[] bucketZeroForDifferentShardsAndStrategies = testShardsAndStrategies()
                .map(shardAndStrategy -> SweepableBucket.of(shardAndStrategy, 0))
                .toArray(SweepableBucket[]::new);
        assertBucketsMapToDifferentCells(bucketZeroForDifferentShardsAndStrategies);
    }

    @ParameterizedTest
    @MethodSource("testShardsAndStrategies")
    public void cellMatchesHistoricalCellMappings(ShardAndStrategy shardAndStrategy) {
        assertThat(DefaultBucketKeySerializer.INSTANCE.bucketToCell(SweepableBucket.of(shardAndStrategy, 0)))
                .isEqualTo(GOLDEN_CELLS.get(shardAndStrategy));
    }

    private static void assertBucketsMapToDifferentCells(SweepableBucket... buckets) {
        List<Cell> cells = Arrays.stream(buckets)
                .map(DefaultBucketKeySerializer.INSTANCE::bucketToCell)
                .collect(Collectors.toList());
        assertThat(cells).doesNotHaveDuplicates();
    }

    private static Stream<ShardAndStrategy> testShardsAndStrategies() {
        return GOLDEN_CELLS.keySet().stream();
    }
}
