/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.sweep.queue;

import org.immutables.value.Value;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;

@Value.Immutable
public abstract class ShardAndStrategy {
    public abstract int shard();
    public abstract TableMetadataPersistence.SweepStrategy strategy();

    public boolean isConservative() {
        return strategy() == TableMetadataPersistence.SweepStrategy.CONSERVATIVE;
    }

    public static ShardAndStrategy of(int shard, TableMetadataPersistence.SweepStrategy sweepStrategy) {
        return ImmutableShardAndStrategy.builder()
                .shard(shard)
                .strategy(sweepStrategy)
                .build();
    }

    public static ShardAndStrategy conservative(int shard) {
        return ShardAndStrategy.of(shard, TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
    }

    public static ShardAndStrategy thorough(int shard) {
        return ShardAndStrategy.of(shard, TableMetadataPersistence.SweepStrategy.THOROUGH);
    }
}
