/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import java.util.function.BooleanSupplier;

public final class LastSeenCommitTsLoader {
    private final ShardProgress shardProgress;
    private final BooleanSupplier isInitializedSupplier;

    public LastSeenCommitTsLoader(ShardProgress shardProgress, BooleanSupplier isInitializedSupplier) {
        this.isInitializedSupplier = isInitializedSupplier;
        this.shardProgress = shardProgress;
    }

    public long getLastSeenCommitTs() {
        if (isInitializedSupplier.getAsBoolean()) {
            return shardProgress.getLastSeenCommitTimestamp();
        }

        // Since we cannot see the accurate state of last seen commit, it is safer to fail all read transactions that
        // do not have a known state in the _txn2 table.
        return Long.MAX_VALUE;
    }
}
