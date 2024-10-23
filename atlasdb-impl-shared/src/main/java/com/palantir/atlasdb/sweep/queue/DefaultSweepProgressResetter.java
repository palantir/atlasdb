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

package com.palantir.atlasdb.sweep.queue;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;

public class DefaultSweepProgressResetter implements SweepProgressResetter {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultSweepProgressResetter.class);
    private final KeyValueService keyValueService;
    private final TableReference bucketProgressTable;
    private final TableReference sweepBucketAssignedTable;
    private final ShardProgress progress;
    private final Supplier<Integer> numberOfShardsSupplier;

    public DefaultSweepProgressResetter(
            KeyValueService keyValueService,
            TableReference bucketProgressTable,
            TableReference sweepBucketAssignedTable,
            ShardProgress progress,
            Supplier<Integer> numberOfShardsSupplier) {
        this.keyValueService = keyValueService;
        this.bucketProgressTable = bucketProgressTable;
        this.sweepBucketAssignedTable = sweepBucketAssignedTable;
        this.progress = progress;
        this.numberOfShardsSupplier = numberOfShardsSupplier;
    }

    @Override
    public void resetProgress(Collection<SweeperStrategy> strategies) {
        log.info("Truncating bucket progress and sweep bucket assigned tables as part of resetting sweep progress");
        keyValueService.truncateTables(Set.of(bucketProgressTable, sweepBucketAssignedTable));
        int shards = numberOfShardsSupplier.get();
        log.info(
                "Now attempting to reset sweep progress for strategies {} and {} shards",
                SafeArg.of("numShards", shards),
                SafeArg.of("strategies", strategies));
        for (int shard = 0; shard < shards; shard++) {
            for (SweeperStrategy strategy : strategies) {
                progress.resetProgressForShard(ShardAndStrategy.of(shard, strategy));
            }
        }
        log.info(
                "Sweep progress was reset for {} shards for strategies {}. If you are running your"
                        + " service in an HA configuration, this message by itself does NOT mean that the reset is"
                        + " complete. The reset is only guaranteed to be complete after this message has been printed"
                        + " by ALL nodes.",
                SafeArg.of("numShards", shards),
                SafeArg.of("strategies", strategies));
    }
}
