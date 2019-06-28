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

import org.immutables.value.Value;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;

@Value.Immutable
public abstract class ShardAndStrategy {
    public abstract int shard();
    public abstract TableMetadataPersistence.SweepStrategy strategy();

    @Value.Check
    void allowOnlyConservativeAndThorough() {
        Preconditions.checkArgument(isConservative() || isThorough(), "Sweep strategy should be CONSERVATIVE or "
                + "THOROUGH, but it is %s instead.", strategy());
    }

    public String toText() {
        return "shard " + shard() + " and strategy " + strategy();
    }

    public LockDescriptor toLockDescriptor() {
        return StringLockDescriptor.of(toText());
    }

    public boolean isConservative() {
        return strategy() == TableMetadataPersistence.SweepStrategy.CONSERVATIVE;
    }

    public boolean isThorough() {
        return strategy() == TableMetadataPersistence.SweepStrategy.THOROUGH;
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

    public static ShardAndStrategy fromInfo(PartitionInfo info) {
        if (info.isConservative().isTrue()) {
            return ShardAndStrategy.conservative(info.shard());
        }
        return ShardAndStrategy.thorough(info.shard());
    }
}
