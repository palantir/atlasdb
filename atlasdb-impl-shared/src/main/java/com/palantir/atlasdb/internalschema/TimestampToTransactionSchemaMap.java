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

package com.palantir.atlasdb.internalschema;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.palantir.common.annotation.Output;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

/**
 * A {@link TimestampToTransactionSchemaMap} keeps track of a mapping of timestamp ranges to transactions schema
 * versions. The reason for using such a mapping is that clients may switch between mappings over time for various
 * reasons.
 *
 * {@link TimestampToTransactionSchemaMap#timestampToTransactionsTableSchemaVersion()} is always expected to cover
 * all timestamps. That is, the ranges present should span the range [1, +∞) and be connected.
 * Note that this does not mean that future behaviour is fixed, since in practice this map is often read as part of
 * a {@link com.palantir.atlasdb.coordination.ValueAndBound}. Thus, even though the map will contain as a key some
 * range [C, +∞) for a constant C, it should be valid up to some point V > C - and behaviour at timestamps after V
 * may subsequently be changed.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableTimestampToTransactionSchemaMap.class)
@JsonDeserialize(as = ImmutableTimestampToTransactionSchemaMap.class)
public abstract class TimestampToTransactionSchemaMap {
    private static final Range<Long> ALL_TIMESTAMPS = Range.atLeast(1L);
    private static final TimestampToTransactionSchemaMap INSTANCE_USING_VERSION_1_AT_ALL_TIMESTAMPS
            = of(ImmutableRangeMap.of(ALL_TIMESTAMPS, 1));

    /**
     * Mapping of timestamp ranges to transactions table schema versions, represented as a set.
     *
     * This representation is for serialisation, because a {@link Range} serializes to an object, so it's not
     * permissible for them to be used as keys in a Map; {@link RangeMap} doesn't seem to be supported in Jackson at
     * time of writing.
     */
    @Value.Parameter
    abstract Set<RangeAndValue> timestampToTransactionsTableSchemaVersion();

    @Value.Lazy
    RangeMap<Long, Integer> rangeMapViewOfTimestamps() {
        ImmutableRangeMap.Builder<Long, Integer> builder = new ImmutableRangeMap.Builder<>();
        timestampToTransactionsTableSchemaVersion()
                .forEach(rangeAndValue -> builder.put(rangeAndValue.longRange(), rangeAndValue.value()));
        return builder.build();
    }

    public static TimestampToTransactionSchemaMap initialValue() {
        return INSTANCE_USING_VERSION_1_AT_ALL_TIMESTAMPS;
    }

    public static TimestampToTransactionSchemaMap of(RangeMap<Long, Integer> initialState) {
        return ImmutableTimestampToTransactionSchemaMap.of(
                initialState.asMapOfRanges()
                        .entrySet()
                        .stream()
                        .map(entry -> RangeAndValue.of(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toSet()));
    }

    public int getVersionForTimestamp(long timestamp) {
        return rangeMapViewOfTimestamps().get(timestamp);
    }

    public TimestampToTransactionSchemaMap copyInstallingNewVersion(
            long lowerBoundForNewVersion,
            int newSchemaVersion) {
        RangeAndValue latestEntry = getLatestEntry();
        validateProvidedTimestampBounds(lowerBoundForNewVersion, newSchemaVersion, latestEntry);

        ImmutableRangeMap.Builder<Long, Integer> builder = ImmutableRangeMap.builder();
        copyOldRangesFromPreviousMap(latestEntry, builder);
        addNewRanges(lowerBoundForNewVersion, newSchemaVersion, latestEntry, builder);
        return ImmutableTimestampToTransactionSchemaMap.of(builder.build());
    }

    private static void validateProvidedTimestampBounds(long lowerBoundForNewVersion, int newSchemaVersion,
            RangeAndValue latestEntry) {
        if (lowerBoundForNewVersion < latestEntry.longRange().lowerEndpoint()) {
            throw new SafeIllegalArgumentException("Cannot install a new schema version at an earlier timestamp;"
                    + " attempted to install version {} at {}, but the newest interval is at {}.",
                    SafeArg.of("attemptedNewVersion", newSchemaVersion),
                    SafeArg.of("attemptedLowerBound", lowerBoundForNewVersion),
                    SafeArg.of("existingInterval", latestEntry));
        }
    }

    private void addNewRanges(
            long lowerBoundForNewVersion,
            int newSchemaVersion,
            RangeAndValue latestRangeAndValue,
            @Output ImmutableRangeMap.Builder<Long, Integer> builder) {
        builder.put(Range.closedOpen(latestRangeAndValue.longRange().lowerEndpoint(), lowerBoundForNewVersion),
                latestRangeAndValue.value());
        builder.put(Range.atLeast(lowerBoundForNewVersion), newSchemaVersion);
    }

    private void copyOldRangesFromPreviousMap(
            RangeAndValue latestRangeAndValue,
            @Output ImmutableRangeMap.Builder<Long, Integer> builder) {
        timestampToTransactionsTableSchemaVersion()
                .stream()
                .filter(rangeAndValue -> !rangeAndValue.equals(latestRangeAndValue))
                .forEach(rangeAndValue -> builder.put(rangeAndValue.longRange(), rangeAndValue.value()));
    }

    private RangeAndValue getLatestEntry() {
        return RangeAndValue.fromMapEntry(rangeMapViewOfTimestamps()
                .asDescendingMapOfRanges()
                .entrySet()
                .iterator()
                .next());
    }

    @Value.Check
    public void check() {
        validateCoversPreciselyAllTimestamps(rangeMapViewOfTimestamps());
    }

    private static void validateCoversPreciselyAllTimestamps(RangeMap<Long, Integer> timestampRangeMap) {
        if (timestampRangeMap.asMapOfRanges().isEmpty() || !timestampRangeMap.span().equals(ALL_TIMESTAMPS)) {
            throw new SafeIllegalArgumentException("Attempted to initialize a timestamp to transaction schema map"
                    + " of {}; its span does not cover precisely all timestamps.",
                    SafeArg.of("timestampToTransactionSchemaMap", timestampRangeMap));
        }

        RangeSet<Long> rangesCovered = TreeRangeSet.create(timestampRangeMap.asMapOfRanges().keySet());
        if (rangesCovered.asRanges().size() != 1) {
            throw new SafeIllegalArgumentException("Attempted to initialize a timestamp to transaction schema map"
                    + " of {}. While the span covers all timestamps, some are missing. The disconnected ranges"
                    + " of the provided map were {}.",
                    SafeArg.of("timestampToTransactionSchemaMap", timestampRangeMap),
                    SafeArg.of("disconnectedRanges", rangesCovered));
        }
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableRangeAndValue.class)
    @JsonDeserialize(as = ImmutableRangeAndValue.class)
    interface RangeAndValue {
        @Value.Parameter
        Range<Long> longRange();
        @Value.Parameter
        int value();

        static RangeAndValue of(Range<Long> longRange, int value) {
            return ImmutableRangeAndValue.of(longRange, value);
        }

        static RangeAndValue fromMapEntry(Map.Entry<Range<Long>, Integer> entry) {
            return of(entry.getKey(), entry.getValue());
        }
    }
}
