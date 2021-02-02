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

import com.google.common.annotations.VisibleForTesting;
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
import com.palantir.logsafe.Preconditions;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

/**
 * A PuncherStore implemented as a table in the KeyValueService.
 *
 * @author jweel
 */
public final class KeyValueServicePuncherStore implements PuncherStore {
    private final class InitializingWrapper extends AsyncInitializer implements AutoDelegate_PuncherStore {
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
        keyValueService.createTable(
                AtlasDbConstants.PUNCH_TABLE,
                TableMetadata.internal()
                        .rowMetadata(
                                NameMetadataDescription.create("time", ValueType.VAR_LONG, ValueByteOrder.DESCENDING))
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
        RangeRequest rangeRequest =
                RangeRequest.builder().startRowInclusive(row).batchHint(1).build();
        try (ClosableIterator<RowResult<Value>> result =
                kvs.getRange(AtlasDbConstants.PUNCH_TABLE, rangeRequest, Long.MAX_VALUE)) {
            if (result.hasNext()) {
                return EncodingUtils.decodeUnsignedVarLong(
                        result.next().getColumns().get(COLUMN).getContents());
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
     * <p>
     * Warning: If the given timestamp is low compared to currently given out timestamps, this call may range scan over
     * the entire table. This table tends to grow quickly, so this call can be expensive. If you can bound how far in
     * the past you want to look for, you should instead call {@link #getMillisForTimestampIfNotPunchedBefore(KeyValueService,
     * long, long)}.
     *
     * @param kvs       the KVS to query.
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

        try (ClosableIterator<RowResult<Value>> result =
                kvs.getRange(AtlasDbConstants.PUNCH_TABLE, rangeRequest, timestampExclusive)) {
            if (result.hasNext()) {
                byte[] encodedMillis = result.next().getRowName();
                EncodingUtils.flipAllBitsInPlace(encodedMillis);
                return EncodingUtils.decodeUnsignedVarLong(encodedMillis);
            } else {
                return 0L;
            }
        }
    }

    public static Optional<Long> getMillisForTimestampWithinBounds(
            KeyValueService kvs, long timestamp,
            MillisAndTimestamp lowerBound, long upperBound) {
        if (lowerBound == null) {
            return getMillisForTimestampSafe(kvs, timestamp, upperBound);
        }

        if (lowerBound.timestamp() == timestamp) {
            return Optional.of(lowerBound.millis());
        }

        BoundsOrMillis boundsOrMillis = getBoundsForRangeScan(
                kvs,
                timestamp,
                ImmutableBounds.builder().lower(lowerBound.millis()).upper(upperBound).build(),
                lowerBound.timestamp());
        if (!boundsOrMillis.bounds().isPresent()) {
            return boundsOrMillis.millis();
        }
        Optional<Long> rangeScanResult =
                getMillisForTimestampBounded(kvs, timestamp, boundsOrMillis.bounds().get());
        if (!rangeScanResult.isPresent()) {
            return boundsOrMillis.millis();
        }
        return rangeScanResult;
    }

    @VisibleForTesting
    public static MillisAndTimestamp findOlder(KeyValueService kvs, long ts, long upperBound) {
        long candidate = upperBound;
        long offset = TimeUnit.DAYS.toMillis(1);

        do {
            MillisAndTimestamp result = getOlder(kvs, candidate);
            if (result.timestamp() <= ts) {
                return result;
            } else {
                candidate = candidate - offset;
                offset = offset * 2;
            }
        } while (candidate >= 0);

        return ImmutableMillisAndTimestamp.builder().millis(0L).timestamp(0L).build();
    }

    @VisibleForTesting
    public static MillisAndTimestamp getOlder(KeyValueService kvs, long millis) {
        byte[] row = EncodingUtils.encodeUnsignedVarLong(millis);
        EncodingUtils.flipAllBitsInPlace(row);
        RangeRequest rangeRequest =
                RangeRequest.builder().startRowInclusive(row).batchHint(1).build();
        try (ClosableIterator<RowResult<Value>> result =
                kvs.getRange(AtlasDbConstants.PUNCH_TABLE, rangeRequest, Long.MAX_VALUE)) {
            if (result.hasNext()) {
                RowResult<Value> rowResult = result.next();
                byte[] rowName = rowResult.getRowName();
                EncodingUtils.flipAllBitsInPlace(rowName);
                return ImmutableMillisAndTimestamp.builder()
                        .millis(EncodingUtils.decodeUnsignedVarLong(rowName))
                        .timestamp(EncodingUtils.decodeUnsignedVarLong(
                                rowResult.getColumns().get(COLUMN).getContents())).build();
            } else {
                return ImmutableMillisAndTimestamp.builder().millis(millis).timestamp(0L).build();
            }
        }
    }

    private static Optional<Long> getMillisForTimestampSafe(KeyValueService kvs, long timestamp, long upperBound) {
        MillisAndTimestamp lowerBound = findOlder(kvs, timestamp, upperBound);
        return getMillisForTimestampWithinBounds(kvs, timestamp, lowerBound, upperBound);
    }

    private static BoundsOrMillis getBoundsForRangeScan(
            KeyValueService kvs, long timestamp, Bounds initialBounds,
            long lowerBoundTimestamp) {
        MillisAndMaybeTimestamp upperBound = ImmutableMillisAndMaybeTimestamp.builder()
                .millis(initialBounds.upper())
                .build();
        MillisAndMaybeTimestamp lowerBound = ImmutableMillisAndMaybeTimestamp.builder()
                .millis(initialBounds.lower())
                .timestamp(lowerBoundTimestamp)
                .build();
        long testValue = Math.max(initialBounds.lower(), initialBounds.upper() - TimeUnit.DAYS.toMillis(7));
        Optional<Long> millisLowEstimate = Optional.empty();
        while (upperBound.millis() - lowerBound.millis() > TimeUnit.DAYS.toMillis(1)) {
            MillisAndTimestamp betweenBounds = getOlder(kvs, testValue);
            if (betweenBounds.timestamp() == timestamp) {
                return ImmutableBoundsOrMillis.builder().millis(betweenBounds.millis()).build();
            } else if (betweenBounds.timestamp() > timestamp) {
                upperBound = ImmutableMillisAndMaybeTimestamp.builder()
                        .millis(testValue)
                        .timestamp(betweenBounds.timestamp())
                        .build();
            } else {
                millisLowEstimate = Optional.of(betweenBounds.millis());
                lowerBound = ImmutableMillisAndMaybeTimestamp.builder()
                        .millis(testValue)
                        .timestamp(betweenBounds.timestamp())
                        .build();
            }
            if (lowerBound.timestamp().isPresent() && upperBound.timestamp().isPresent()) {
                long tsDelta = upperBound.timestamp().get() - lowerBound.timestamp().get();
                long millisDelta = upperBound.millis() - lowerBound.millis();

                testValue = millisDelta * (timestamp - lowerBound.timestamp().get()) / tsDelta + lowerBound.millis();
            } else {
                testValue = (lowerBound.millis() + upperBound.millis()) / 2;
            }
        }
        return ImmutableBoundsOrMillis.builder()
                .bounds(ImmutableBounds.builder().lower(lowerBound.millis() - 1).upper(upperBound.millis()).build())
                .millis(millisLowEstimate)
                .build();
    }

    private static Optional<Long> getMillisForTimestampBounded(KeyValueService kvs, long timestamp, Bounds bounds) {
        long timestampExclusive = timestamp + 1;
        byte[] startRow = EncodingUtils.encodeUnsignedVarLong(bounds.upper());
        byte[] endRow = EncodingUtils.encodeUnsignedVarLong(Math.max(0, bounds.lower()));
        EncodingUtils.flipAllBitsInPlace(startRow);
        EncodingUtils.flipAllBitsInPlace(endRow);

        RangeRequest rangeRequest = RangeRequest.builder()
                .startRowInclusive(startRow)
                .endRowExclusive(endRow)
                .retainColumns(ImmutableList.of(COLUMN))
                .batchHint(1000)
                .build();

        try (ClosableIterator<RowResult<Value>> result =
                kvs.getRange(AtlasDbConstants.PUNCH_TABLE, rangeRequest, timestampExclusive)) {
            if (result.hasNext()) {
                byte[] encodedMillis = result.next().getRowName();
                EncodingUtils.flipAllBitsInPlace(encodedMillis);
                return Optional.of(EncodingUtils.decodeUnsignedVarLong(encodedMillis));
            } else {
                return Optional.empty();
            }
        }
    }

    /**
     * Same as {@link #getMillisForTimestamp(KeyValueService, long)}, except that it first does a lookup for the first
     * timestamp punched before lowerBound. If that value is lower than timestamp, we then look up the real time value
     * in the KVS, otherwise, we return lowerBound to avoid doing a large range scan.
     *
     * @param kvs        the KVS to query.
     * @param timestamp  timestamp to query for.
     * @param lowerBound if the first timestamp punched before this real time is larger than the query timestamp, then
     *                   do not do a range scan and instead return lowerBound.
     */
    public static long getMillisForTimestampIfNotPunchedBefore(KeyValueService kvs, long timestamp, long lowerBound) {
        if (get(kvs, Math.max(0L, lowerBound)) < timestamp) {
            return getMillisForTimestamp(kvs, timestamp);
        } else {
            return lowerBound;
        }
    }

    @Immutable
    public interface MillisAndMaybeTimestamp {
        long millis();
        Optional<Long> timestamp();
    }

    @Immutable
    public interface Bounds {
        long upper();
        long lower();
    }

    @Immutable
    public interface BoundsOrMillis {
        Optional<Bounds> bounds();
        Optional<Long> millis();

        @Check
        default void check() {
            Preconditions.checkArgument(bounds().isPresent() || millis().isPresent());
        }
    }
}
