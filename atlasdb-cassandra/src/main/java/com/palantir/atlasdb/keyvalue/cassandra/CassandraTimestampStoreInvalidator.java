/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.common.annotation.Idempotent;
import com.palantir.timestamp.TimestampStoreInvalidator;

public final class CassandraTimestampStoreInvalidator implements TimestampStoreInvalidator {
    private final CassandraTimestampBackupRunner backupRunner;

    private CassandraTimestampStoreInvalidator(CassandraTimestampBackupRunner backupRunner) {
        this.backupRunner = backupRunner;
    }

    public static CassandraTimestampStoreInvalidator create(CassandraKeyValueService keyValueService) {
        return new CassandraTimestampStoreInvalidator(new CassandraTimestampBackupRunner(keyValueService));
    }

    @Override
    @Idempotent
    public long backupAndInvalidate() {
        backupRunner.ensureTimestampTableExists();
        return backupRunner.backupExistingTimestamp();
    }

    @Override
    @Idempotent
    public void revalidateFromBackup() {
        backupRunner.ensureTimestampTableExists();
        backupRunner.restoreFromBackup();
    }
}
