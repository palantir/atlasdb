/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.persistentlock;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowResult;

@Value.Immutable
public abstract class LockEntry {
    public static final String DELIMITER = "_";

    @Value.Parameter
    public abstract PersistentLockName lockName();

    @Value.Parameter
    public abstract long lockId();

    @Value.Parameter
    public abstract String reason();

    public static LockEntry of(PersistentLockName lockName, long lockId, String reason) {
        return ImmutableLockEntry.of(lockName, lockId, reason);
    }

    public static LockEntry of(PersistentLockName lockName, long lockId) {
        return ImmutableLockEntry.of(lockName, lockId, "");
    }

    public static LockEntry fromRowResult(RowResult<com.palantir.atlasdb.keyvalue.api.Value> rowResult) {
        String rowName = new String(rowResult.getRowName(), StandardCharsets.UTF_8);
        String reason = new String(rowResult.getOnlyColumnValue().getContents(), StandardCharsets.UTF_8);

        return ImmutableLockEntry.of(extractLockName(rowName), extractLockId(rowName), reason);
    }

    public Map<Cell, byte[]> insertionMap() {
        return ImmutableMap.of(lockTableCell(), asUtf8(reason()));
    }

    public Multimap<Cell, Long> deletionMapWithTimestamp(long timestamp) {
        return ImmutableMultimap.of(lockTableCell(), timestamp);
    }

    private Cell lockTableCell() {
        byte[] rowName = makeLockRowName(lockName(), lockId());
        byte[] columnName = asUtf8("reasonForLock");
        return Cell.create(rowName, columnName);
    }

    private byte[] makeLockRowName(PersistentLockName lock, long thisId) {
        return asUtf8(lock.name() + DELIMITER + thisId);
    }

    private byte[] asUtf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static PersistentLockName extractLockName(String rowName) {
        String[] rowNameParts = rowName.split(DELIMITER);
        return PersistentLockName.of(rowNameParts[0]);
    }

    private static long extractLockId(String rowName) {
        String[] rowNameParts = rowName.split(DELIMITER);
        return Long.parseLong(rowNameParts[1]);
    }
}
