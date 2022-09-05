/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep.queue;

import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ShardAndStrategy {
    public abstract int shard();

    public abstract SweeperStrategy strategy();

    public String toText() {
        return "shard " + shard() + " and strategy " + strategy();
    }

    public LockDescriptor toLockDescriptor() {
        return StringLockDescriptor.of(toText());
    }

    public boolean conservativeFlag() {
        return strategy() == SweeperStrategy.CONSERVATIVE || strategy() == SweeperStrategy.NON_SWEEPABLE;
    }

    public boolean nonSweepableFlag() {
        return strategy() == SweeperStrategy.NON_SWEEPABLE;
    }

    public boolean isThorough() {
        return strategy() == SweeperStrategy.THOROUGH;
    }

    public static ShardAndStrategy of(int shard, SweeperStrategy sweepStrategy) {
        return ImmutableShardAndStrategy.builder()
                .shard(shard)
                .strategy(sweepStrategy)
                .build();
    }

    public static ShardAndStrategy conservative(int shard) {
        return ShardAndStrategy.of(shard, SweeperStrategy.CONSERVATIVE);
    }

    public static ShardAndStrategy thorough(int shard) {
        return ShardAndStrategy.of(shard, SweeperStrategy.THOROUGH);
    }

    public static ShardAndStrategy nonSweepable() {
        return SweepQueueUtils.NON_SWEEPABLE;
    }
}
