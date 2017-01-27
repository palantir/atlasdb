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

import java.util.Map;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.timestamp.TimestampStoreInvalidator;

public class CassandraTimestampStoreInvalidator implements TimestampStoreInvalidator {
    private static final long DEFAULT_TIMESTAMP_BOUND = 0L;

    private final CassandraKeyValueService rawCassandraKvs;
    private final CassandraTimestampCqlExecutor cassandraTimestampCqlExecutor;

    public CassandraTimestampStoreInvalidator(CassandraKeyValueService rawCassandraKvs) {
        this(rawCassandraKvs, new CassandraTimestampCqlExecutor(rawCassandraKvs));
    }

    @VisibleForTesting
    CassandraTimestampStoreInvalidator(CassandraKeyValueService rawCassandraKvs,
            CassandraTimestampCqlExecutor cassandraTimestampCqlExecutor) {
        this.rawCassandraKvs = rawCassandraKvs;
        this.cassandraTimestampCqlExecutor = cassandraTimestampCqlExecutor;
    }

    @Override
    public void invalidateTimestampStore() {
        ensureTimestampTableExists();
        Optional<Long> bound = getCurrentTimestampBound();
        bound.ifPresent(cassandraTimestampCqlExecutor::backupBound);
    }

    @Override
    public void revalidateTimestampStore() {
        ensureTimestampTableExists();
        Optional<Long> bound = getBackupTimestampBound();
        bound.ifPresent(cassandraTimestampCqlExecutor::restoreBoundFromBackup);
    }

    private void ensureTimestampTableExists() {
        rawCassandraKvs.createTable(AtlasDbConstants.TIMESTAMP_TABLE,
                CassandraTimestampBoundStore.TIMESTAMP_TABLE_METADATA.persistToBytes());
    }

    private Optional<Long> getCurrentTimestampBound() {
        return getBoundFromKvs(CassandraTimestampCqlExecutor.ROW_AND_COLUMN_NAME_BYTES);
    }

    private Optional<Long> getBackupTimestampBound() {
        return getBoundFromKvs(CassandraTimestampCqlExecutor.BACKUP_COLUMN_NAME_BYTES);
    }

    private Optional<Long> getBoundFromKvs(byte[] timestampCellName) {
        Cell timestampCell = Cell.create(timestampCellName, timestampCellName);
        Map<Cell, Value> result = rawCassandraKvs.get(
                AtlasDbConstants.TIMESTAMP_TABLE,
                ImmutableMap.of(timestampCell, Long.MAX_VALUE));
        if (result.isEmpty()) {
            return Optional.of(DEFAULT_TIMESTAMP_BOUND);
        }

        Value value = Iterables.getOnlyElement(result.values());
        return getLongFromValue(value);
    }

    private Optional<Long> getLongFromValue(Value value) {
        byte[] contents = value.getContents();
        if (contents.length == 0) {
            return Optional.empty();
        }
        return Optional.of(PtBytes.toLong(contents));
    }
}
