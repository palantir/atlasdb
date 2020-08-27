/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.cleaner;

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
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.base.ClosableIterator;
import java.nio.charset.StandardCharsets;

/**
 * A PuncherStore implemented as a table in the KeyValueService.
 *
 * @author jweel
 */
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
        keyValueService.createTable(AtlasDbConstants.PUNCH_TABLE, TableMetadata.internal()
                .rowMetadata(NameMetadataDescription.create("time", ValueType.VAR_LONG, ValueByteOrder.DESCENDING))
                .singleNamedColumn("t", "t", ValueType.VAR_LONG)
                .build()
                .persistToBytes());
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
        return get(keyValueService, timeMillis);
    }

    public static Long get(KeyValueService kvs, Long timeMillis) {
        byte[] row = EncodingUtils.encodeUnsignedVarLong(timeMillis);
        EncodingUtils.flipAllBitsInPlace(row);
        RangeRequest rangeRequest = RangeRequest.builder().startRowInclusive(row).batchHint(1).build();
        try (ClosableIterator<RowResult<Value>> result = kvs.getRange(AtlasDbConstants.PUNCH_TABLE, rangeRequest,
                Long.MAX_VALUE)) {
            if (result.hasNext()) {
                return EncodingUtils.decodeUnsignedVarLong(result.next().getColumns().get(COLUMN).getContents());
            } else {
                return Long.MIN_VALUE;
            }
        }
    }

    @Override
    public long getMillisForTimestamp(long timestamp) {
        return getMillisForTimestamp(keyValueService, timestamp);
    }

    /**
     * Returns the real time in milliseconds corresponding to the given timestamp.
     *
     * Warning: If the given timestamp is low compared to currently given out timestamps, this call may range scan over
     * the entire table. This table tends to grow quickly, so this call can be expensive. If you can bound how far in
     * the past you want to look for, you should instead call
     * {@link #getMillisForTimestampIfNotPunchedBefore(KeyValueService, long, long)}.
     *
     * @param kvs the KVS to query.
     * @param timestamp timestamp to query for.
     */
    public static long getMillisForTimestamp(KeyValueService kvs, long timestamp) {
        long timestampExclusive = timestamp + 1;
        byte[] startRow = EncodingUtils.encodeUnsignedVarLong(Long.MAX_VALUE);
        EncodingUtils.flipAllBitsInPlace(startRow);
        RangeRequest rangeRequest = RangeRequest.builder()
                .startRowInclusive(startRow)
                .retainColumns(ImmutableList.of(COLUMN))
                .batchHint(1000)
                .build();

        try (ClosableIterator<RowResult<Value>> result = kvs.getRange(AtlasDbConstants.PUNCH_TABLE, rangeRequest,
                timestampExclusive)) {
            if (result.hasNext()) {
                byte[] encodedMillis = result.next().getRowName();
                EncodingUtils.flipAllBitsInPlace(encodedMillis);
                return EncodingUtils.decodeUnsignedVarLong(encodedMillis);
            } else {
                return 0L;
            }
        }
    }

    /**
     * Same as {@link #getMillisForTimestamp(KeyValueService, long)}, except that it first does a lookup for the
     * first timestamp punched before lowerBound. If that value is lower than timestamp, we then look up the real time
     * value in the KVS, otherwise, we return lowerBound to avoid doing a large range scan.
     *
     * @param kvs the KVS to query.
     * @param timestamp timestamp to query for.
     * @param lowerBound if the first timestamp punched before this real time is larger than the query timestamp, then
     * do not do a range scan and instead return lowerBound.
     */
    public static long getMillisForTimestampIfNotPunchedBefore(KeyValueService kvs, long timestamp, long lowerBound) {
        if (get(kvs, Math.max(0L, lowerBound)) < timestamp) {
            return getMillisForTimestamp(kvs, timestamp);
        } else {
            return lowerBound;
        }
    }
}
