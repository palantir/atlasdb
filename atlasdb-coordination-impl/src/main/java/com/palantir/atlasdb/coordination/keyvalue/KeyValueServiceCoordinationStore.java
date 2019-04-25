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

package com.palantir.atlasdb.coordination.keyvalue;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.coordination.AutoDelegate_CoordinationStore;
import com.palantir.atlasdb.coordination.CoordinationStore;
import com.palantir.atlasdb.coordination.SequenceAndBound;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.atlasdb.keyvalue.impl.ImmutableCheckAndSetResult;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

/**
 * An implementation of {@link CoordinationStore} that persists its state in a {@link KeyValueService}.
 *
 * Specifically, a {@link KeyValueServiceCoordinationStore} stores its data in a single row in the
 * {@link AtlasDbConstants#COORDINATION_TABLE}. This specific row is identified by the store's
 * {@link KeyValueServiceCoordinationStore#coordinationRow}; the reason for doing this is that it allows
 * for multiple distinct sequences to coexist in one table.
 *
 * Within a single identified row, values are written with dynamic column keys as sequence numbers, which are
 * positive longs. Furthermore, a {@link SequenceAndBound} which controls which of the values is considered to be
 * the most recent in the coordination sequence and how long it is valid for is written at dynamic column key zero.
 * This {@link SequenceAndBound} should contain the ID of the value that is considered to be the most current
 * in the sequence.
 *
 * Sequence numbers are expected to be unique and increasing, in that if one requests a sequence number, the result
 * returned should be larger than any sequence number returned by any call which finished before our call started.
 */
// TODO (jkong): Coordination stores should be able to clean up old values.
public final class KeyValueServiceCoordinationStore<T> implements CoordinationStore<T> {
    @VisibleForTesting
    class InitializingWrapper extends AsyncInitializer implements AutoDelegate_CoordinationStore<T> {
        @Override
        protected void tryInitialize() {
            KeyValueServiceCoordinationStore.this.tryInitialize();
        }

        @Override
        protected String getInitializingClassName() {
            return KeyValueServiceCoordinationStore.class.getSimpleName();
        }

        @Override
        public CoordinationStore<T> delegate() {
            checkInitialized();
            return KeyValueServiceCoordinationStore.this;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(KeyValueServiceCoordinationStore.class);

    private static final TableMetadata COORDINATION_TABLE_METADATA = TableMetadata.internal()
            .singleSafeRowComponent("sequence", ValueType.BLOB)
            .singleDynamicSafeColumn("sequenceNumber", ValueType.VAR_LONG, ValueType.BLOB)
            .sweepStrategy(SweepStrategy.NOTHING)
            .build();

    private static final long ADVANCEMENT_QUANTUM = 5_000_000L;
    private static final int MAX_ATTEMPTS_TO_READ = 10;

    private static final String COORDINATION_SEQUENCE_AND_BOUND_DESCRIPTION = "coordination sequence and bound";
    private static final String VALUE_DESCRIPTION = "value for coordination service";

    private final ObjectMapper objectMapper;
    private final KeyValueService kvs;
    private final byte[] coordinationRow;
    private final LongSupplier sequenceNumberSupplier;
    private final BiPredicate<T, T> shouldReuseExtantValue;
    private final Class<T> clazz;

    @VisibleForTesting
    final InitializingWrapper wrapper = new InitializingWrapper();

    @VisibleForTesting
    KeyValueServiceCoordinationStore(
            ObjectMapper objectMapper,
            KeyValueService kvs,
            byte[] coordinationRow,
            LongSupplier sequenceNumberSupplier,
            BiPredicate<T, T> shouldReuseExtantValue,
            Class<T> clazz) {
        this.objectMapper = objectMapper;
        this.kvs = kvs;
        this.coordinationRow = coordinationRow;
        this.sequenceNumberSupplier = sequenceNumberSupplier;
        this.shouldReuseExtantValue = shouldReuseExtantValue;
        this.clazz = clazz;
    }

    public static <T> CoordinationStore<T> create(
            ObjectMapper objectMapper,
            KeyValueService kvs,
            byte[] coordinationRow,
            LongSupplier sequenceNumberSupplier,
            BiPredicate<T, T> shouldReuseExistingValue,
            Class<T> clazz,
            boolean initializeAsync) {
        KeyValueServiceCoordinationStore<T> coordinationStore
                = new KeyValueServiceCoordinationStore<>(
                        objectMapper, kvs, coordinationRow, sequenceNumberSupplier, shouldReuseExistingValue, clazz);
        coordinationStore.wrapper.initialize(initializeAsync);
        return coordinationStore.isInitialized() ? coordinationStore : coordinationStore.wrapper;
    }

    @Override
    public boolean isInitialized() {
        return wrapper.isInitialized();
    }

    // TODO (jkong): Since apart from the coordination value all other entries are immutable, we can perform
    // some caching.
    @Override
    public Optional<ValueAndBound<T>> getAgreedValue() {
        for (int attemptNumber = 0; attemptNumber < MAX_ATTEMPTS_TO_READ; attemptNumber++) {
            Optional<SequenceAndBound> coordinationValue = getCoordinationValue();
            if (!coordinationValue.isPresent()) {
                return Optional.empty();
            }
            SequenceAndBound sequenceAndBound = coordinationValue.get();
            Optional<T> value = getValue(sequenceAndBound.sequence());
            if (value.isPresent()) {
                return Optional.of(ValueAndBound.of(value, sequenceAndBound.bound()));
            }
            // The value was swept out underneath us. We will retry, a new value should be agreed.
        }
        log.warn("Coordination store failed to get an agreed value after {} attempts to read a value",
                SafeArg.of("numAttempts", MAX_ATTEMPTS_TO_READ));
        throw new IllegalStateException("The coordination store seemed to be repeatedly swept out from underneath"
                + " us. Attempted " + MAX_ATTEMPTS_TO_READ + " reads, but all failed!");
    }

    /**
     * In case of failure, the returned value and bound are guaranteed to be the ones that were in the KVS at the time
     * of CAS failure only if {@link KeyValueService#getCheckAndSetCompatibility()} is
     * {@link com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility#SUPPORTED_DETAIL_ON_FAILURE}.
     *
     * The coordination store has an invariant that sequence numbers assigned to values cannot progress backwards.
     * This holds, because the only transformations that change the sequence number of the active value call the
     * sequence number supplier for a fresh sequence number, and these happen after reading the value in the
     * KVS; this value was written only after the previous update got a fresh sequence number. Thus, it is safe
     * to, upon successful CAS of the {@link SequenceAndBound}, delete all entries from sequence number 1 to the
     * sequence number (exclusive).
     *
     * The catch here is that there may be inflight reads that will now return a {@link ValueAndBound} where the
     * value is {@link Optional#empty()}, while this should never have happened in the past.
     */
    @Override
    public CheckAndSetResult<ValueAndBound<T>> transformAgreedValue(Function<ValueAndBound<T>, T> transform) {
        Optional<SequenceAndBound> coordinationValue = getCoordinationValue();
        ValueAndBound<T> extantValueAndBound = ValueAndBound.of(coordinationValue.flatMap(
                sequenceAndBound -> getValue(sequenceAndBound.sequence())),
                coordinationValue.map(SequenceAndBound::bound).orElse(SequenceAndBound.INVALID_BOUND));
        T targetValue = transform.apply(extantValueAndBound);

        SequenceAndBound newSequenceAndBound
                = determineNewSequenceAndBound(coordinationValue, extantValueAndBound, targetValue);

        coordinationValue.ifPresent(existingCoordinationValue -> Preconditions.checkState(
                newSequenceAndBound.sequence() >= existingCoordinationValue.sequence(),
                "Coordination service looks like it's trying to go back in time, from %s to %s,"
                        + " which is not allowed",
                newSequenceAndBound.sequence(),
                existingCoordinationValue.sequence()));

        CheckAndSetResult<SequenceAndBound> casResult = checkAndSetCoordinationValue(
                coordinationValue, newSequenceAndBound);
        if (casResult.successful()) {
            // cleanup values between 1 and newSequenceAndBound.sequence exclusive
            // TODO (jkong): Implement column range deletes
            // should be async?
       }
        return extractRelevantValues(targetValue, newSequenceAndBound.bound(), casResult);
    }

    private void tryInitialize() {
        kvs.createTable(AtlasDbConstants.COORDINATION_TABLE, COORDINATION_TABLE_METADATA.persistToBytes());
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType") // Passed from other operations returning Optional
    private SequenceAndBound determineNewSequenceAndBound(
            Optional<SequenceAndBound> coordinationValue,
            ValueAndBound<T> extantValueAndBound,
            T targetValue) {
        long sequenceNumber;
        long newBound;
        if (reuseExtantValue(coordinationValue, extantValueAndBound.value(), targetValue)) {
            // Safe as we're only on this branch if the value is present
            sequenceNumber = coordinationValue.get().sequence();
            long freshSequenceNumber = sequenceNumberSupplier.getAsLong();
            newBound = freshSequenceNumber < extantValueAndBound.bound()
                    ? extantValueAndBound.bound()
                    : getNewBound(freshSequenceNumber);
        } else {
            sequenceNumber = sequenceNumberSupplier.getAsLong();
            putUnlessValueExists(sequenceNumber, targetValue);
            newBound = getNewBound(sequenceNumber);
        }
        return SequenceAndBound.of(sequenceNumber, newBound);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType") // Passed from other operations returning Optional
    private boolean reuseExtantValue(
            Optional<SequenceAndBound> coordinationValue,
            Optional<T> extantValue,
            T targetValue) {
        return coordinationValue.isPresent()
                && extantValue.map(presentValue -> shouldReuseExtantValue.test(presentValue, targetValue))
                .orElse(false);
    }

    private CheckAndSetResult<ValueAndBound<T>> extractRelevantValues(T targetValue, long newBound,
            CheckAndSetResult<SequenceAndBound> casResult) {
        if (casResult.successful()) {
            return CheckAndSetResult.of(true, ImmutableList.of(ValueAndBound.of(Optional.of(targetValue), newBound)));
        }
        return CheckAndSetResult.of(
                false,
                casResult.existingValues()
                        .stream()
                        .map(value -> ValueAndBound.of(getValue(value.sequence()), value.bound()))
                        .collect(Collectors.toList()));
    }

    private long getNewBound(long pointInTime) {
        return pointInTime + ADVANCEMENT_QUANTUM;
    }

    @VisibleForTesting
    Optional<T> getValue(long sequenceNumber) {
        Preconditions.checkState(
                sequenceNumber > 0,
                "Only positive sequence numbers are supported, but found %s",
                sequenceNumber);
        return readFromCoordinationTable(getCellForSequence(sequenceNumber))
                .map(Value::getContents)
                .map(this::deserializeValue);
    }

    @VisibleForTesting
    void putUnlessValueExists(long sequenceNumber, T value) {
        Preconditions.checkState(
                sequenceNumber > 0,
                "Only positive sequence numbers are supported, but found %s",
                sequenceNumber);
        try {
            kvs.putUnlessExists(AtlasDbConstants.COORDINATION_TABLE,
                    ImmutableMap.of(getCellForSequence(sequenceNumber), serializeValue(value)));
        } catch (KeyAlreadyExistsException e) {
            throw new SafeIllegalStateException("The coordination store failed a putUnlessExists. This is unexpected"
                    + " as it implies timestamps may have been reused, or a writer to the store behaved badly."
                    + " The offending sequence number was {}. "
                    + " Please contact support - DO NOT ATTEMPT TO FIX THIS YOURSELF.",
                    e,
                    SafeArg.of("sequenceNumber", sequenceNumber));
        }
    }

    @VisibleForTesting
    Optional<SequenceAndBound> getCoordinationValue() {
        return readFromCoordinationTable(getCoordinationValueCell())
                .map(Value::getContents)
                .map(this::deserializeSequenceAndBound);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @VisibleForTesting
    CheckAndSetResult<SequenceAndBound> checkAndSetCoordinationValue(
            Optional<SequenceAndBound> oldValue, SequenceAndBound newValue) {
        if (oldValue.map(presentOldValue -> Objects.equals(presentOldValue, newValue)).orElse(false)) {
            return ImmutableCheckAndSetResult.of(true, ImmutableList.of(newValue));
        }

        CheckAndSetRequest request = new CheckAndSetRequest.Builder()
                .table(AtlasDbConstants.COORDINATION_TABLE)
                .cell(getCoordinationValueCell())
                .oldValue(oldValue.map(this::serializeSequenceAndBound))
                .newValue(serializeSequenceAndBound(newValue))
                .build();

        try {
            kvs.checkAndSet(request);
            return ImmutableCheckAndSetResult.of(true, ImmutableList.of(newValue));
        } catch (CheckAndSetException e) {
            if (e.getActualValues() != null) {
                return ImmutableCheckAndSetResult.of(false, e.getActualValues()
                        .stream()
                        .map(this::deserializeSequenceAndBound)
                        .collect(Collectors.toList()));
            }
            return getFailedResultWithMostRecentValueAndBound(oldValue, newValue);
        }
    }

    private SequenceAndBound deserializeSequenceAndBound(byte[] contents) {
        return deserializeData(contents, SequenceAndBound.class, COORDINATION_SEQUENCE_AND_BOUND_DESCRIPTION);
    }

    private T deserializeValue(byte[] contents) {
        return deserializeData(contents, clazz, VALUE_DESCRIPTION);
    }

    private <T1> T1 deserializeData(byte[] data, Class<T1> valueClass, String safeDescriptionOfItemToDeserialize) {
        try {
            return objectMapper.readValue(data, valueClass);
        } catch (IOException e) {
            log.error("Error encountered when deserializing {}: {}",
                    SafeArg.of("safeDescriptionOfItemToDeserialize", safeDescriptionOfItemToDeserialize),
                    SafeArg.of("coordinationData", PtBytes.toString(data)));
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    byte[] serializeSequenceAndBound(SequenceAndBound sequenceAndBound) {
        return serializeData(sequenceAndBound, COORDINATION_SEQUENCE_AND_BOUND_DESCRIPTION);
    }

    private byte[] serializeValue(T value) {
        return serializeData(value, VALUE_DESCRIPTION);
    }

    private <T1> byte[] serializeData(T1 object, String safeDescriptionOfItemToSerialize) {
        try {
            return objectMapper.writeValueAsBytes(object);
        } catch (JsonProcessingException e) {
            log.error("Error encountered when serializing {}: {}",
                    SafeArg.of("safeDescriptionOfItemToSerialize", safeDescriptionOfItemToSerialize),
                    SafeArg.of("coordinationData", object));
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    Cell getCoordinationValueCell() {
        return getCellForSequence(0);
    }

    private Cell getCellForSequence(long sequenceNumber) {
        return Cell.create(coordinationRow, ValueType.VAR_LONG.convertFromJava(sequenceNumber));
    }

    private Optional<Value> readFromCoordinationTable(Cell cell) {
        return Optional.ofNullable(kvs.get(AtlasDbConstants.COORDINATION_TABLE, ImmutableMap.of(cell, Long.MAX_VALUE))
                .get(cell));
    }

    private CheckAndSetResult<SequenceAndBound> getFailedResultWithMostRecentValueAndBound(
            Optional<SequenceAndBound> oldValue, SequenceAndBound newValue) {
        Optional<SequenceAndBound> actualValue = getCoordinationValue();
        if (!actualValue.isPresent()) {
            throw new SafeIllegalStateException("Failed to check and set coordination value from {} to {}, but "
                    + "there is no value present in the coordination table",
                    SafeArg.of("oldValue", oldValue), SafeArg.of("newValue", newValue));
        }
        return ImmutableCheckAndSetResult.of(false, ImmutableList.of(actualValue.get()));
    }
}
