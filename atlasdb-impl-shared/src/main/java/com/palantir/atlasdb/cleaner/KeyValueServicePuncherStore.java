/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.cleaner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.base.ClosableIterator;

/**
 * A PuncherStore implemented as a table in the KeyValueService.
 *
 * @author jweel
 */
public class KeyValueServicePuncherStore implements PuncherStore {
    private static final byte[] COLUMN = "t".getBytes();

    public static KeyValueServicePuncherStore create(KeyValueService keyValueService) {
        keyValueService.createTable(AtlasDbConstants.PUNCH_TABLE, new TableMetadata(
                NameMetadataDescription.create(ImmutableList.of(new NameComponentDescription("time", ValueType.VAR_LONG, ValueByteOrder.DESCENDING))),
                new ColumnMetadataDescription(ImmutableList.of(
                        new NamedColumnDescription("t", "t", ColumnValueDescription.forType(ValueType.VAR_LONG)))),
                        ConflictHandler.IGNORE_ALL).persistToBytes());
        return new KeyValueServicePuncherStore(keyValueService);
    }

    private final KeyValueService keyValueService;

    private KeyValueServicePuncherStore(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    @Override
    public void put(long timestamp, long timeMillis) {
        byte[] row = EncodingUtils.encodeUnsignedVarLong(timeMillis);
        EncodingUtils.flipAllBitsInPlace(row);
        Cell cell = Cell.create(row, COLUMN);
        byte[] value = EncodingUtils.encodeUnsignedVarLong(timestamp);
        keyValueService.put(AtlasDbConstants.PUNCH_TABLE, ImmutableMap.of(cell, value), timestamp);
    }

    @Override
    public Long get(Long timeMillis) {
        byte[] row = EncodingUtils.encodeUnsignedVarLong(timeMillis);
        EncodingUtils.flipAllBitsInPlace(row);
        RangeRequest rangeRequest =
                RangeRequest.builder().startRowInclusive(row).batchHint(1).build();
        ClosableIterator<RowResult<Value>> result =
                keyValueService.getRange(AtlasDbConstants.PUNCH_TABLE, rangeRequest, Long.MAX_VALUE);
        try {
            if (result.hasNext()) {
                return EncodingUtils.decodeUnsignedVarLong(result.next().getColumns().get(COLUMN).getContents());
            } else {
                return Long.MIN_VALUE;
            }
        } finally {
            result.close();
        }
    }
}
