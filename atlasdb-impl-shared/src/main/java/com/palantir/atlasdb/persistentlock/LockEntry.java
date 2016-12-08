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
import java.util.Optional;
import java.util.SortedMap;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowResult;

@Value.Immutable
public abstract class LockEntry {
    public static final String DELIMITER = "_";
    public static final String REASON_FOR_LOCK_COLUMN = "reasonForLock";
    public static final String EXCLUSIVE_COLUMN = "exclusive";
    public static final String TOMBSTONE_COLUMN = "tombstone";

    @Value.Parameter
    public abstract PersistentLockName lockName();

    @Value.Parameter
    public abstract long lockId();

    @Value.Parameter
    public abstract String reason();

    @Value.Parameter
    public abstract boolean exclusive();

    @Value.Default
    public boolean tombstoned() {
        return false;
    }

    public static LockEntry of(PersistentLockName lockName, long lockId, String reason, boolean exclusive) {
        return ImmutableLockEntry.of(lockName, lockId, reason, exclusive);
    }

    public static LockEntry of(PersistentLockName lockName, long lockId, String reason) {
        return ImmutableLockEntry.of(lockName, lockId, reason, true);
    }

    public static LockEntry of(PersistentLockName lockName, long lockId) {
        return ImmutableLockEntry.of(lockName, lockId, "");
    }

    public static LockEntry fromRowResult(RowResult<com.palantir.atlasdb.keyvalue.api.Value> rowResult) {
        String rowName = asString(rowResult.getRowName());
        String reason = valueOfColumnInRow(REASON_FOR_LOCK_COLUMN, rowResult).get();
        boolean exclusive = Boolean.parseBoolean(valueOfColumnInRow(EXCLUSIVE_COLUMN, rowResult).get());
        boolean tombstoned = Boolean.parseBoolean(valueOfColumnInRow(TOMBSTONE_COLUMN, rowResult).orElse("false"));

        return ImmutableLockEntry.builder()
                .lockName(extractLockName(rowName))
                .lockId(extractLockId(rowName))
                .reason(reason)
                .exclusive(exclusive)
                .tombstoned(tombstoned)
                .build();
    }

    public Map<Cell, byte[]> insertionMap() {
        return ImmutableMap.of(
                reasonCell(), asUtf8Bytes(reason()),
                exclusiveCell(), asUtf8Bytes(Boolean.toString(exclusive())));
    }

    public Map<Cell, byte[]> writeTombstoneMap() {
        return ImmutableMap.of(tombstoneCell(), asUtf8Bytes(Boolean.toString(true)));
    }

    public Multimap<Cell, Long> deletionMapWithTimestamp(long timestamp) {
        return ImmutableMultimap.of(
                reasonCell(), timestamp,
                exclusiveCell(), timestamp,
                tombstoneCell(), timestamp);
    }

    private static Optional<String> valueOfColumnInRow(
            String columnName,
            RowResult<com.palantir.atlasdb.keyvalue.api.Value> rowResult) {
        byte[] columnNameBytes = asUtf8Bytes(columnName);
        SortedMap<byte[], com.palantir.atlasdb.keyvalue.api.Value> columns = rowResult.getColumns();
        if (columns.containsKey(columnNameBytes)) {
            byte[] contents = columns.get(columnNameBytes).getContents();
            return Optional.of(asString(contents));
        } else {
            return Optional.empty();
        }
    }

    private Cell reasonCell() {
        return makeColumnCell(REASON_FOR_LOCK_COLUMN);
    }

    private Cell exclusiveCell() {
        return makeColumnCell(EXCLUSIVE_COLUMN);
    }

    private Cell tombstoneCell() {
        return makeColumnCell(TOMBSTONE_COLUMN);
    }

    private Cell makeColumnCell(String columnName) {
        byte[] rowBytes = makeLockRowName(lockName(), lockId());
        byte[] columnBytes = asUtf8Bytes(columnName);
        return Cell.create(rowBytes, columnBytes);
    }

    private byte[] makeLockRowName(PersistentLockName lock, long thisId) {
        return asUtf8Bytes(lock.name() + DELIMITER + thisId);
    }

    private static byte[] asUtf8Bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String asString(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
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
