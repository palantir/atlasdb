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
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.logsafe.Preconditions;
import com.palantir.timestamp.TimestampStoreInvalidator;

public class DbTimestampStoreInvalidator implements TimestampStoreInvalidator {
    private final InvalidationRunner invalidationRunner;

    public DbTimestampStoreInvalidator(
            ConnectionManagerAwareDbKvs kvs, TableReference timestampTable, String tablePrefixString) {
        this.invalidationRunner = new InvalidationRunner(kvs.getConnectionManager(), timestampTable, tablePrefixString);
    }

    public static TimestampStoreInvalidator create(
            KeyValueService kvs, TableReference timestampTable, String tablePrefixString) {
        Preconditions.checkArgument(
                kvs instanceof ConnectionManagerAwareDbKvs,
                "DbTimestampStoreInvalidator should be instantiated with a ConnectionManagerAwareDbKvs!");
        return new DbTimestampStoreInvalidator((ConnectionManagerAwareDbKvs) kvs, timestampTable, tablePrefixString);
    }

    @Override
    public long backupAndInvalidate() {
        invalidationRunner.createTableIfDoesNotExist();
        return invalidationRunner.ensureInDbStoreIsPoisonedAndGetLastAllocatedTimestamp();
    }
}
