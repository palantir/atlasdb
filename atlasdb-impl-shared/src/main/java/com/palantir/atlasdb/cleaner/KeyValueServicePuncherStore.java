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
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

/**
 * A PuncherStore implemented as a table in the KeyValueService.
 *
 * @author jweel
 */
public final class KeyValueServicePuncherStore implements PuncherStore {
    private static final SafeLogger log = SafeLoggerFactory.get(KeyValueServicePuncherStore.class);
    static final long MAX_RANGE_SCAN_SIZE = TimeUnit.HOURS.toMillis(4);

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
        byte[] row = createRow(timeMillis);
        Cell cell = Cell.create(row, COLUMN);
        byte[] value = EncodingUtils.encodeUnsignedVarLong(timestamp);
        keyValueService.put(AtlasDbConstants.PUNCH_TABLE, ImmutableMap.of(cell, value), timestamp);
    }

    @Override
    public Long get(Long timeMillis) {
        return get(keyValueService, timeMillis);
    }

    public static Long get(KeyValueService kvs, Long timeMillis) {
        try (ClosableIterator<RowResult<Value>> result = getFirstRowBefore(kvs, timeMillis)) {
            if (result.hasNext()) {
                return EncodingUtils.decodeUnsignedVarLong(getColumn(result.next()));
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
     * the entire table. This table tends to grow quickly, so this call can be expensive.
     *
     * You should call
     * {@link #getMillisForTimestampWithinBounds(KeyValueService, long, MillisAndMaybeTimestamp, long)} instead.
     *
     * @param kvs the KVS to query.
     * @param timestamp timestamp to query for.
     */
    public static long getMillisForTimestamp(KeyValueService kvs, long timestamp) {
        long timestampExclusive = timestamp + 1;
        byte[] startRow = createRow(Long.MAX_VALUE);
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

    /**
     * An optimised version of {@link #getMillisForTimestamp(KeyValueService, long)}, which uses a modified version
     * of binary search to reduce the range that is scanned down to at most {@link #MAX_RANGE_SCAN_SIZE}.
     *
     * Note that since this method uses binary search, in the presence of clock drift in the punch table it is
     * possible, though unlikely, that the returned value is different than if using
     * {@link #getMillisForTimestamp(KeyValueService, long)}.
     *
     * @param kvs the KVS to query.
     * @param timestamp timestamp to query for.
     * @param lowerBound a known milliseconds time corresponding to a lower timestamp, to use as a lower bound.
     *                   A null value can be used if not available.
     * @param upperBound the upper bound on real time in milliseconds, usually current time
     *
     * @return the upper bound in milliseconds corresponding to the given timestamp, or empty if no entries are punched
     * within the specified bounds.
     */
    public static Optional<MillisAndMaybeTimestamp> getMillisForTimestampWithinBounds(
            KeyValueService kvs, long timestamp, MillisAndMaybeTimestamp lowerBound, long upperBound) {
        MillisAndMaybeTimestamp lowerBoundToUse =
                Optional.ofNullable(lowerBound).orElseGet(() -> findOlder(kvs, timestamp, upperBound));

        if (lowerBoundToUse.timestampSatisfies(ts -> ts == timestamp)) {
            return Optional.of(lowerBoundToUse);
        }

        BoundsOrMillis boundsOrMillis = getBoundsForRangeScan(
                kvs,
                timestamp,
                ImmutableBounds.builder()
                        .lower(lowerBoundToUse.millis())
                        .upper(upperBound)
                        .build(),
                lowerBoundToUse.timestamp());
        if (!boundsOrMillis.bounds().isPresent()) {
            return boundsOrMillis.millis();
        }

        Optional<MillisAndMaybeTimestamp> rangeScanResult =
                boundsOrMillis.bounds().flatMap(bounds -> getMillisForTimestampBounded(kvs, timestamp, bounds));
        if (rangeScanResult.isPresent()) {
            return rangeScanResult;
        }
        if (!boundsOrMillis.millis().isPresent()) {
            log.info(
                    "Did not find a match for timestamp {} with initial bounds {} and {}. Bounds for range scan were "
                            + "{}.",
                    SafeArg.of("timestamp", timestamp),
                    SafeArg.of("initialLower", lowerBound),
                    SafeArg.of("initialUpper", upperBound),
                    SafeArg.of("rangeScanBounds", boundsOrMillis.bounds()));
        }
        return boundsOrMillis.millis();
    }

    /**
     * Attempts to efficiently find a time in milliseconds before upperBound at which a lower entry than timestamp
     * was punched. The result is not guaranteed to be the greatest such value.
     */
    @VisibleForTesting
    static MillisAndMaybeTimestamp findOlder(KeyValueService kvs, long timestamp, long upperBound) {
        long candidate = upperBound;
        long offset = MAX_RANGE_SCAN_SIZE;

        do {
            MillisAndMaybeTimestamp result = getOlder(kvs, candidate);
            if (!result.timestampSatisfies(ts -> ts > timestamp)) {
                return result;
            } else {
                candidate = candidate - offset;
                offset = offset * 2;
            }
        } while (candidate >= 0);

        return ImmutableMillisAndMaybeTimestamp.builder().millis(0L).build();
    }

    /**
     * Returns the greatest time in milliseconds with an entry in the punch store that is less than or equal to millis,
     * and its associated timestamp, if it exists. Otherwise, the returned value will contain millis and no timestamp
     * denoting that millis can be used as a lower bound for any range scan.
     */
    @VisibleForTesting
    static MillisAndMaybeTimestamp getOlder(KeyValueService kvs, long millis) {
        try (ClosableIterator<RowResult<Value>> result = getFirstRowBefore(kvs, millis)) {
            return extract(result).orElseGet(() -> ImmutableMillisAndMaybeTimestamp.builder()
                    .millis(millis)
                    .build());
        }
    }

    private static ClosableIterator<RowResult<Value>> getFirstRowBefore(KeyValueService kvs, long millis) {
        RangeRequest rangeRequest = RangeRequest.builder()
                .startRowInclusive(createRow(millis))
                .batchHint(1)
                .build();
        return kvs.getRange(AtlasDbConstants.PUNCH_TABLE, rangeRequest, Long.MAX_VALUE);
    }

    private static byte[] createRow(long millis) {
        byte[] row = EncodingUtils.encodeUnsignedVarLong(millis);
        EncodingUtils.flipAllBitsInPlace(row);
        return row;
    }

    private static Optional<MillisAndMaybeTimestamp> extract(Iterator<RowResult<Value>> result) {
        if (result.hasNext()) {
            RowResult<Value> rowResult = result.next();
            byte[] rowName = rowResult.getRowName();
            EncodingUtils.flipAllBitsInPlace(rowName);
            return Optional.of(ImmutableMillisAndMaybeTimestamp.builder()
                    .millis(EncodingUtils.decodeUnsignedVarLong(rowName))
                    .timestamp(EncodingUtils.decodeUnsignedVarLong(getColumn(rowResult)))
                    .build());
        }
        return Optional.empty();
    }

    private static byte[] getColumn(RowResult<Value> rowResult) {
        return rowResult.getColumns().get(COLUMN).getContents();
    }

    /**
     * Modified binary search to determine a range to scan to find the real time in milliseconds corresponding to
     * timestamp. In each iteration, we look up the closest existing entry to the test value in milliseconds. There
     * are three options:
     *
     *   1) the result has an associated ts equal to timestamp; we have found an exact match and no range scan is needed
     *   2) the result has an associated ts greater than timestamp; there is no need to range scan after result.millis()
     *   (note that this is generally less than testValue)
     *   3) otherwise; there is no need to range scan before testValue, but if there is no valid entry in the range
     *   then betweenBounds.millis() is the best approximation for the real time in milliseconds corresponding to
     *   timestamp.
     *
     * Once both the upper bound and the lower bound have an associated timestamp, with a probability of 0.5 we
     * determine the next test value by assuming a linear correlation between time in milliseconds and timestamps, and
     * interpolate the next candidate in the standard way: y = d(x)/d(y) (x - x_1) + y_1. Otherwise, we simply take the
     * midpoint.
     *
     * Note  that, as an optimisation, the initial test value is expected to be equal to lower bound in this
     * implementation, but in cases lower bound is extremely low, the test value will be relatively close to the upper
     * bound.
     *
     * @return bounds for range scan if range scan should be performed, and a value in milliseconds that is the best
     * approximation if the range scan returns no results.
     */
    private static BoundsOrMillis getBoundsForRangeScan(
            KeyValueService kvs, long timestamp, Bounds initialBounds, Optional<Long> lowerBoundTimestamp) {
        MillisAndMaybeTimestamp upperBound = ImmutableMillisAndMaybeTimestamp.builder()
                .millis(initialBounds.upper())
                .build();
        MillisAndMaybeTimestamp lowerBound = ImmutableMillisAndMaybeTimestamp.builder()
                .millis(initialBounds.lower())
                .timestamp(lowerBoundTimestamp)
                .build();
        long testValue = Math.max(initialBounds.lower(), initialBounds.upper() - 7 * MAX_RANGE_SCAN_SIZE);
        Optional<MillisAndMaybeTimestamp> millisLowEstimate = Optional.empty();
        while (upperBound.millis() - lowerBound.millis() > MAX_RANGE_SCAN_SIZE) {
            MillisAndMaybeTimestamp betweenBounds = getOlder(kvs, testValue);

            if (betweenBounds.timestampSatisfies(ts -> ts == timestamp)) {
                return noRangeScan(betweenBounds);
            } else if (betweenBounds.timestampSatisfies(ts -> ts > timestamp)) {
                upperBound = betweenBounds;
            } else {
                millisLowEstimate = Optional.of(betweenBounds);
                lowerBound = ImmutableMillisAndMaybeTimestamp.builder()
                        .from(betweenBounds)
                        .millis(testValue)
                        .build();
            }

            if (lowerBound.timestamp().isPresent()
                    && upperBound.timestamp().isPresent()
                    && ThreadLocalRandom.current().nextBoolean()) {
                long tsDelta =
                        upperBound.timestamp().get() - lowerBound.timestamp().get();
                long millisDelta = upperBound.millis() - lowerBound.millis();
                if (tsDelta == 0 || millisDelta == 0) {
                    return noRangeScan(lowerBound);
                }
                testValue = millisDelta * (timestamp - lowerBound.timestamp().get()) / tsDelta + lowerBound.millis();
                if (testValue < lowerBound.millis() || testValue > upperBound.millis()) {
                    testValue = (lowerBound.millis() + upperBound.millis()) / 2;
                }
            } else {
                testValue = (lowerBound.millis() + upperBound.millis()) / 2;
            }
        }
        return ImmutableBoundsOrMillis.builder()
                .bounds(ImmutableBounds.builder()
                        .lower(Math.max(lowerBound.millis() - 1, 0))
                        .upper(upperBound.millis())
                        .build())
                .millis(millisLowEstimate)
                .build();
    }

    private static Optional<MillisAndMaybeTimestamp> getMillisForTimestampBounded(
            KeyValueService kvs, long timestamp, Bounds bounds) {
        RangeRequest rangeRequest = RangeRequest.builder()
                .startRowInclusive(createRow(bounds.upper()))
                .endRowExclusive(createRow(bounds.lower()))
                .retainColumns(ImmutableList.of(COLUMN))
                .batchHint(1000)
                .build();

        try (ClosableIterator<RowResult<Value>> result =
                kvs.getRange(AtlasDbConstants.PUNCH_TABLE, rangeRequest, timestamp + 1)) {
            return extract(result);
        }
    }

    private static ImmutableBoundsOrMillis noRangeScan(MillisAndMaybeTimestamp betweenBounds) {
        return ImmutableBoundsOrMillis.builder().millis(betweenBounds).build();
    }

    @Immutable
    public interface Bounds {
        long upper();

        long lower();
    }

    @Immutable
    public interface BoundsOrMillis {
        Optional<Bounds> bounds();

        Optional<MillisAndMaybeTimestamp> millis();

        @Check
        default void check() {
            Preconditions.checkArgument(bounds().isPresent() || millis().isPresent());
        }
    }
}
