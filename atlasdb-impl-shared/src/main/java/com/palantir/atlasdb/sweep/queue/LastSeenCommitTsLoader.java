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

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.table.description.Schemas;

public final class LastSeenCommitTsLoader {
    private final ShardProgress shardProgress;
    private final KeyValueService kvs;
    private volatile boolean isInitialized = false;

    public LastSeenCommitTsLoader(KeyValueService kvs) {
        this.kvs = kvs;
        this.shardProgress = new ShardProgress(kvs);
    }

    public long getLastSeenCommitTs() {
        if (isInitialized) {
            return shardProgress.getLastSeenCommitTimestamp();
        }

        if (kvs.isInitialized()) {
            Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
            isInitialized = true;
            return shardProgress.getLastSeenCommitTimestamp();
        }

        // Since we cannot see the accurate state of last seen commit, it is safer to fail all read transactions that
        // do not have a known state in the _txn2 table.
        return Long.MAX_VALUE;
    }
}
