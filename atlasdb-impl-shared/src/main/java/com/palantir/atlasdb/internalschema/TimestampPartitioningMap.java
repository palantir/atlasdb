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
 * A {@link TimestampPartitioningMap} keeps track of a mapping of timestamp ranges to values.
 *
 * {@link TimestampPartitioningMap#timestampMappings()} is always expected to cover all timestamps. That is, the ranges
 * present should span the range [1, +∞) and be connected.
 *
 * Note that this does not mean that future behaviour is fixed, since in practice this map is often read as part of
 * a {@link com.palantir.atlasdb.coordination.ValueAndBound}. Thus, even though the map will contain as a key some
 * range [C, +∞) for a constant C, it should be valid up to some point V > C - and behaviour at timestamps after V
 * may subsequently be changed.
 *
 * @param <T> type of values timestamp ranges map to
 */
@Value.Immutable
@JsonSerialize(as = ImmutableTimestampPartitioningMap.class)
@JsonDeserialize(as = ImmutableTimestampPartitioningMap.class)
public abstract class TimestampPartitioningMap<T> {
    private static final Range<Long> ALL_TIMESTAMPS = Range.atLeast(1L);

    /**
     * Set representation of range-value mappings.
     * This representation is for serialisation, because a {@link Range} serializes to an object, so it's not
     * permissible for them to be used as keys in a Map; {@link RangeMap} doesn't seem to be supported in Jackson at
     * time of writing.
     */
    @Value.Parameter
    abstract Set<RangeAndValue<T>> timestampMappings();

    @Value.Lazy
    public RangeMap<Long, T> rangeMapView() {
        ImmutableRangeMap.Builder<Long, T> builder = new ImmutableRangeMap.Builder<>();
        timestampMappings()
                .forEach(rangeAndValue -> builder.put(rangeAndValue.longRange(), rangeAndValue.value()));
        return builder.build();
    }

    public static <T> TimestampPartitioningMap<T> of(RangeMap<Long, T> initialState) {
        return ImmutableTimestampPartitioningMap.of(
                initialState.asMapOfRanges()
                        .entrySet()
                        .stream()
                        .map(entry -> RangeAndValue.of(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toSet()));
    }

    public T getValueForTimestamp(long timestamp) {
        return rangeMapView().get(timestamp);
    }

    public TimestampPartitioningMap<T> copyInstallingNewValue(
            long lowerBoundForNewVersion,
            T newValue) {
        RangeAndValue<T> latestEntry = getLatestEntry();
        validateProvidedTimestampBounds(lowerBoundForNewVersion, newValue, latestEntry);

        ImmutableRangeMap.Builder<Long, T> builder = ImmutableRangeMap.builder();
        copyOldRangesFromPreviousMap(latestEntry, builder);
        addNewRanges(lowerBoundForNewVersion, newValue, latestEntry, builder);
        return ImmutableTimestampPartitioningMap.of(builder.build());
    }

    private static <T> void validateProvidedTimestampBounds(long lowerBoundForNewValue, T newValue,
            RangeAndValue<T> latestEntry) {
        if (lowerBoundForNewValue < latestEntry.longRange().lowerEndpoint()) {
            throw new SafeIllegalArgumentException("Cannot install a new value at an earlier timestamp;"
                    + " attempted to install version {} at {}, but the newest interval is at {}.",
                    SafeArg.of("attemptedNewValue", newValue),
                    SafeArg.of("attemptedLowerBound", lowerBoundForNewValue),
                    SafeArg.of("existingInterval", latestEntry));
        }
    }

    private void addNewRanges(
            long lowerBoundForNewVersion,
            T newValue,
            RangeAndValue<T> latestRangeAndValue,
            @Output ImmutableRangeMap.Builder<Long, T> builder) {
        builder.put(Range.closedOpen(latestRangeAndValue.longRange().lowerEndpoint(), lowerBoundForNewVersion),
                latestRangeAndValue.value());
        builder.put(Range.atLeast(lowerBoundForNewVersion), newValue);
    }

    private void copyOldRangesFromPreviousMap(
            RangeAndValue latestRangeAndValue,
            @Output ImmutableRangeMap.Builder<Long, T> builder) {
        timestampMappings()
                .stream()
                .filter(rangeAndValue -> !rangeAndValue.equals(latestRangeAndValue))
                .forEach(rangeAndValue -> builder.put(rangeAndValue.longRange(), rangeAndValue.value()));
    }

    private RangeAndValue<T> getLatestEntry() {
        return RangeAndValue.fromMapEntry(rangeMapView()
                .asDescendingMapOfRanges()
                .entrySet()
                .iterator()
                .next());
    }

    @Value.Check
    public void check() {
        validateCoversPreciselyAllTimestamps(rangeMapView());
    }

    private static <T> void validateCoversPreciselyAllTimestamps(RangeMap<Long, T> timestampRangeMap) {
        if (timestampRangeMap.asMapOfRanges().isEmpty() || !timestampRangeMap.span().equals(ALL_TIMESTAMPS)) {
            throw new SafeIllegalArgumentException("Attempted to initialize a timestamp partitioning map"
                    + " of {}; its span does not cover precisely all timestamps.",
                    SafeArg.of("timestampToTransactionSchemaMap", timestampRangeMap));
        }

        RangeSet<Long> rangesCovered = TreeRangeSet.create(timestampRangeMap.asMapOfRanges().keySet());
        if (rangesCovered.asRanges().size() != 1) {
            throw new SafeIllegalArgumentException("Attempted to initialize a timestamp partitioning map"
                    + " of {}. While the span covers all timestamps, some are missing. The disconnected ranges"
                    + " of the provided map were {}.",
                    SafeArg.of("timestampToTransactionSchemaMap", timestampRangeMap),
                    SafeArg.of("disconnectedRanges", rangesCovered));
        }
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableRangeAndValue.class)
    @JsonDeserialize(as = ImmutableRangeAndValue.class)
    interface RangeAndValue<T> {
        @Value.Parameter
        Range<Long> longRange();
        @Value.Parameter
        T value();

        static <T> RangeAndValue<T> of(Range<Long> longRange, T value) {
            return ImmutableRangeAndValue.of(longRange, value);
        }

        static <T> RangeAndValue<T> fromMapEntry(Map.Entry<Range<Long>, T> entry) {
            return of(entry.getKey(), entry.getValue());
        }
    }
}
