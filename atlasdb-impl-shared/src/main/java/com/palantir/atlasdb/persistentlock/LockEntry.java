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
package com.palantir.atlasdb.persistentlock;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;

@Value.Immutable
public abstract class LockEntry {
    private static final String REASON_FOR_LOCK_COLUMN = "reasonForLock";
    private static final String LOCK_ID_COLUMN = "lockId";

    public abstract String rowName();
    public abstract String lockId();
    public abstract String reason();

    public Map<Cell, byte[]> insertionMap() {
        return ImmutableMap.of(
                makeCell(LOCK_ID_COLUMN), asUtf8Bytes(lockId()),
                makeCell(REASON_FOR_LOCK_COLUMN), asUtf8Bytes(reason()));
    }

    public Multimap<Cell, Long> deletionMap() {
        Long timestamp = AtlasDbConstants.TRANSACTION_TS;
        return ImmutableMultimap.of(
                makeCell(LOCK_ID_COLUMN), timestamp,
                makeCell(REASON_FOR_LOCK_COLUMN), timestamp);
    }

    private Cell makeCell(String columnName) {
        byte[] rowBytes = String.valueOf(rowName()).getBytes(StandardCharsets.UTF_8);
        byte[] columnBytes = columnName.getBytes(StandardCharsets.UTF_8);
        return Cell.create(rowBytes, columnBytes);
    }

    private byte[] asUtf8Bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
