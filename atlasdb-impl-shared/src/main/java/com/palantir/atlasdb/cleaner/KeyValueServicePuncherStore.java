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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.base.ClosableIterator;
import com.palantir.processors.AutoDelegate;

/**
 * A PuncherStore implemented as a table in the KeyValueService.
 *
 * This utilises dynamic columns to remove the necessity of range scans.
 *
 * The schema is:
 *
 * row: time rounded to 14 days
 * column: rest of the timestamp, with inverted bits to ensure reverse range scans
 * value: Atlas timestamp
 *
 * In order to look up a value for a give time, truncate in the same way and range scan within the row
 * for the first cell beneath the provided time.
 *
 * Selecting the wall clock timestamp for a given Atlas timestamp still requires a full table scan,
 * but should not be used in production code.
 *
 * It is expected that each row contains (with default setting) (1440 * 14 = 20k cells).
 *
 * Note one caveat of this implementation - as written, the get method only works if it is in the last
 * 420 days, and defaults to Long.MIN_VALUE otherwise.
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

    @VisibleForTesting
    static final long MILLIS_IN_TWO_WEEKS = TimeUnit.DAYS.toMillis(14);

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
                                .componentName("time_rounded_millis")
                                .type(ValueType.FIXED_LONG_LITTLE_ENDIAN)
                                .build())),
                new ColumnMetadataDescription(new DynamicColumnDescription(
                        NameMetadataDescription.create(ImmutableList.of(new NameComponentDescription.Builder()
                                .componentName("remainder_millis")
                                .type(ValueType.VAR_LONG)
                                .build())),
                        ColumnValueDescription.forType(ValueType.VAR_LONG))),
                ConflictHandler.IGNORE_ALL,
                TableMetadataPersistence.LogSafety.SAFE).persistToBytes());
    }

    @Override
    public boolean isInitialized() {
        return wrapper.isInitialized();
    }

    @Override
    public void put(long timestamp, long timeMillis) {
        Cell cell = Cell.create(row(timeMillis), column(timeMillis));
        byte[] value = EncodingUtils.encodeUnsignedVarLong(timestamp);
        keyValueService.put(AtlasDbConstants.PUNCH_TABLE, ImmutableMap.of(cell, value), timestamp);
    }

    @Override
    public Long get(Long timeMillis) {
        byte[] row = row(timeMillis);
        byte[] column = column(timeMillis);
        BatchColumnRangeSelection rangeSelection = BatchColumnRangeSelection.create(column, null, 1);
        Iterator<Map.Entry<Cell, Value>> iterator = keyValueService.getRowsColumnRange(
                AtlasDbConstants.PUNCH_TABLE,
                ImmutableList.of(row),
                rangeSelection,
                Long.MAX_VALUE).get(row);

        if (iterator.hasNext()) {
            return getAtlasTimestamp(iterator.next().getValue());
        }

        return getHistoric(timeMillis, 1)
                .orElseGet(() -> getHistoric(timeMillis, 30).orElse(Long.MIN_VALUE));
    }

    @Override
    public long getMillisForTimestamp(long timestamp) {
        return getMillisForTimestamp(keyValueService, timestamp);
    }

    private Optional<Long> getHistoric(long timeMillis, int numRowsToCheck) {
        List<byte[]> rows = LongStream.rangeClosed(1, numRowsToCheck)
                .map(i -> timeMillis - i * MILLIS_IN_TWO_WEEKS)
                .mapToObj(KeyValueServicePuncherStore::row)
                .collect(Collectors.toList());
        Map<byte[], RowColumnRangeIterator> itMap = keyValueService.getRowsColumnRange(
                AtlasDbConstants.PUNCH_TABLE,
                rows,
                BatchColumnRangeSelection.create(null, null, 1),
                Long.MAX_VALUE);

        return rows.stream()
                .map(itMap::get)
                .filter(Iterator::hasNext)
                .map(Iterator::next)
                .map(Map.Entry::getValue)
                .map(KeyValueServicePuncherStore::getAtlasTimestamp)
                .findFirst();
    }

    private static long getAtlasTimestamp(Value value) {
        return EncodingUtils.decodeUnsignedVarLong(value.getContents());
    }

    // Relies on a full table scan, but should only be used in a cli.
    public static long getMillisForTimestamp(KeyValueService kvs, long timestamp) {
        long timestampExclusive = timestamp + 1;
        RangeRequest rangeRequest = RangeRequest.builder().batchHint(1).build();
        try (ClosableIterator<RowResult<Value>> result =
                kvs.getRange(AtlasDbConstants.PUNCH_TABLE, rangeRequest, timestampExclusive)) {
            // With Guava 21.0, can do Streams.stream(result), Streams.stream(row.getCells())
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(result, 0), false)
                    .flatMap(row -> StreamSupport.stream(row.getCells().spliterator(), false))
                    .map(Map.Entry::getKey)
                    .mapToLong(KeyValueServicePuncherStore::timestamp)
                    .max()
                    .orElse(0L);
        }
    }

    private static long timestamp(Cell cell) {
        return MILLIS_IN_TWO_WEEKS * EncodingUtils.decodeLittleEndian(cell.getRowName(), 0)
                + EncodingUtils.decodeUnsignedVarLong(EncodingUtils.flipAllBits(cell.getColumnName()));
    }

    // floorDiv and floorMod are to ensure that this works properly if the time is somehow negative.
    private static byte[] column(long timeMillis) {
        byte[] ret = EncodingUtils.encodeUnsignedVarLong(Math.floorMod(timeMillis, MILLIS_IN_TWO_WEEKS));
        EncodingUtils.flipAllBitsInPlace(ret);
        return ret;
    }

    private static byte[] row(long timeMillis) {
        return EncodingUtils.encodeLittleEndian(Math.floorDiv(timeMillis, MILLIS_IN_TWO_WEEKS));
    }
}
