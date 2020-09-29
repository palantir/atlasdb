/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.logsafe.Preconditions;
import com.palantir.timestamp.TimestampStoreInvalidator;

public class DbTimestampStoreInvalidator implements TimestampStoreInvalidator {
    private final DbKvs kvs;
    private final InDbTimestampBoundStore store;

    public DbTimestampStoreInvalidator(DbKvs kvs, InDbTimestampBoundStore store) {
        this.kvs = kvs;
        this.store = store;
    }

    public static TimestampStoreInvalidator create(KeyValueService kvs, InDbTimestampBoundStore store) {
        Preconditions.checkArgument(kvs instanceof DbKvs,
                "DbTimestampStoreInvalidator should be instantiated with a DbKvs!");
        return new DbTimestampStoreInvalidator((DbKvs) kvs, store);
    }

    @Override
    public long backupAndInvalidate() {
        store.init(); // creates a table
        return store.takeBackupAndPoisonTheStore();
    }
}
