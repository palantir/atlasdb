/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import java.nio.charset.StandardCharsets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.async.initializer.AsyncInitializer;
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
import com.palantir.processors.AutoDelegate;

/**
 * A PuncherStore implemented as a table in the KeyValueService.
 *
 * @author jweel
 */
@AutoDelegate(typeToExtend = PuncherStore.class)
public final class KeyValueServicePuncherStore implements PuncherStore {
    private class InitializingWrapper extends AsyncInitializer implements AutoDelegate_PuncherStore {
        @Override
        public PuncherStore delegate() {
            checkInitialized();
            return KeyValueServicePuncherStore.this;
        }

        @Override
        protected void tryInitialize() {
            KeyValueServicePuncherStore.this.tryInitialize();
        }

        @Override
        protected String getInitializingClassName() {
            return "KeyValueServicePuncherStore";
        }
    }

    private static final byte[] COLUMN = "t".getBytes(StandardCharsets.UTF_8);

    private final InitializingWrapper wrapper = new InitializingWrapper();
    private final KeyValueService keyValueService;

    public static PuncherStore create(KeyValueService keyValueService) {
        return create(keyValueService, AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static PuncherStore create(KeyValueService keyValueService, boolean initializeAsync) {
        KeyValueServicePuncherStore puncherStore = new KeyValueServicePuncherStore(keyValueService);
        puncherStore.wrapper.initialize(initializeAsync);
        return puncherStore.wrapper.isInitialized() ? puncherStore : puncherStore.wrapper;
    }

    private KeyValueServicePuncherStore(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    private void tryInitialize() {
        keyValueService.createTable(AtlasDbConstants.PUNCH_TABLE, new TableMetadata(
                NameMetadataDescription.create(ImmutableList.of(
                        new NameComponentDescription.Builder()
                                .componentName("time")
                                .type(ValueType.VAR_LONG)
                                .byteOrder(ValueByteOrder.DESCENDING)
                                .build())),
                new ColumnMetadataDescription(ImmutableList.of(
                        new NamedColumnDescription("t", "t", ColumnValueDescription.forType(ValueType.VAR_LONG)))),
                ConflictHandler.IGNORE_ALL).persistToBytes());
    }

    @Override
    public boolean isInitialized() {
        return wrapper.isInitialized();
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

    @Override
    public long getMillisForTimestamp(long timestamp) {
        return getMillisForTimestamp(keyValueService, timestamp);
    }

    public static long getMillisForTimestamp(KeyValueService kvs, long timestamp) {
        long timestampExclusive = timestamp + 1;
        // punch table is keyed by the real value we're trying to find so we have to do a whole table
        // scan, which is fine because this table should be really small
        byte[] startRow = EncodingUtils.encodeUnsignedVarLong(Long.MAX_VALUE);
        EncodingUtils.flipAllBitsInPlace(startRow);
        RangeRequest rangeRequest =
                RangeRequest.builder().startRowInclusive(startRow).batchHint(1).build();
        ClosableIterator<RowResult<Value>> result =
                kvs.getRange(AtlasDbConstants.PUNCH_TABLE, rangeRequest, timestampExclusive);

        try {
            if (result.hasNext()) {
                byte[] encodedMillis = result.next().getRowName();
                EncodingUtils.flipAllBitsInPlace(encodedMillis);
                return EncodingUtils.decodeUnsignedVarLong(encodedMillis);
            } else {
                return 0L;
            }
        } finally {
            result.close();
        }
    }

}
