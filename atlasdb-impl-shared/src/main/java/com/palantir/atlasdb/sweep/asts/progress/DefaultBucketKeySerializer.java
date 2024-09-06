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

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.schema.generated.SweepBucketProgressTable;
import com.palantir.atlasdb.schema.generated.SweepBucketProgressTable.SweepBucketProgressNamedColumn;
import com.palantir.atlasdb.schema.generated.SweepBucketProgressTable.SweepBucketProgressRow;
import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

enum DefaultBucketKeySerializer {
    INSTANCE;

    Cell bucketToCell(Bucket bucket) {
        SweepBucketProgressTable.SweepBucketProgressRow row = SweepBucketProgressRow.of(
                bucket.shardAndStrategy().shard(),
                bucket.bucketIdentifier(),
                persistStrategy(bucket.shardAndStrategy().strategy()));
        return Cell.create(row.persistToBytes(), SweepBucketProgressNamedColumn.BUCKET_PROGRESS.getShortName());
    }

    private static byte[] persistStrategy(SweeperStrategy strategy) {
        switch (strategy) {
            case THOROUGH:
                return new byte[] {0};
            case CONSERVATIVE:
                return new byte[] {1};
            case NON_SWEEPABLE:
                return new byte[] {2};
            default:
                throw new SafeIllegalStateException(
                        "Unexpected sweeper strategy", SafeArg.of("sweeperStrategy", strategy));
        }
    }
}
