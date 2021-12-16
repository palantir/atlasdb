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
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

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
 */
// TODO (jkong): Coordination stores should be able to clean up old values.
// TODO (jkong): Split this class into 2 or more pieces as it's large.
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

    private static final SafeLogger log = SafeLoggerFactory.get(KeyValueServiceCoordinationStore.class);

    private static final TableMetadata COORDINATION_TABLE_METADATA = TableMetadata.internal()
            .singleSafeRowComponent("sequence", ValueType.BLOB)
            .singleDynamicSafeColumn("sequenceNumber", ValueType.VAR_LONG, ValueType.BLOB)
            .sweepStrategy(SweepStrategy.NOTHING)
            .build();

    private static final long ADVANCEMENT_QUANTUM = 5_000_000L;

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
        KeyValueServiceCoordinationStore<T> coordinationStore = new KeyValueServiceCoordinationStore<>(
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
        return readLatestState().map(CoordinationStoreState::valueAndBound);
    }

    /**
     * In case of failure, the returned value and bound are guaranteed to be the ones that were in the KVS at the time
     * of CAS failure only if {@link KeyValueService#getCheckAndSetCompatibility()} supports detail on failure.
     */
    @Override
    public CheckAndSetResult<ValueAndBound<T>> transformAgreedValue(Function<ValueAndBound<T>, T> transform) {
        Optional<CoordinationStoreState<T>> storeState = readLatestState();

        T targetValue = transform.apply(storeState
                .map(CoordinationStoreState::valueAndBound)
                .orElseGet(() -> ValueAndBound.of(Optional.empty(), SequenceAndBound.INVALID_BOUND)));

        SequenceAndBound newSequenceAndBound = determineNewSequenceAndBound(storeState, targetValue);

        CheckAndSetResult<SequenceAndBound> casResult = checkAndSetCoordinationValue(
                storeState.map(CoordinationStoreState::sequenceAndBound), newSequenceAndBound);
        return extractRelevantValues(targetValue, newSequenceAndBound.bound(), casResult);
    }

    private Optional<CoordinationStoreState<T>> readLatestState() {
        while (true) {
            Optional<SequenceAndBound> coordinationValue = getCoordinationValue();
            if (coordinationValue.isEmpty()) {
                return Optional.empty();
            }
            SequenceAndBound presentCoordinationValue = coordinationValue.get();
            Optional<T> value = getValue(presentCoordinationValue.sequence());
            if (value.isPresent()) {
                CoordinationStoreState<T> coordinationStoreState = CoordinationStoreState.of(
                        presentCoordinationValue.sequence(), presentCoordinationValue.bound(), value);
                return Optional.of(coordinationStoreState);
            }
            log.info(
                    "We read a value from the coordination store that seems to have been deleted, for sequence"
                            + " and bound {}. This may happen as a part of sweeping the coordination store. We will"
                            + " retry. This is acceptable, but if it persists and transactions are not making progress,"
                            + " please contact support.",
                    SafeArg.of("sequenceAndBound", presentCoordinationValue));
        }
    }

    private void tryInitialize() {
        kvs.createTable(AtlasDbConstants.COORDINATION_TABLE, COORDINATION_TABLE_METADATA.persistToBytes());
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType") // Passed from other operations returning Optional
    private SequenceAndBound determineNewSequenceAndBound(Optional<CoordinationStoreState<T>> oldState, T targetValue) {
        long sequenceNumber;
        long newBound;
        if (reuseExtantValue(oldState, targetValue)) {
            // Safe as we're only on this branch if the value is present
            CoordinationStoreState<T> extantState = oldState.get();
            sequenceNumber = extantState.sequence();
            long freshSequenceNumber = sequenceNumberSupplier.getAsLong();
            newBound =
                    freshSequenceNumber < extantState.bound() ? extantState.bound() : getNewBound(freshSequenceNumber);
        } else {
            sequenceNumber = sequenceNumberSupplier.getAsLong();
            putUnlessValueExists(sequenceNumber, targetValue);
            newBound = getNewBound(sequenceNumber);
        }
        return SequenceAndBound.of(sequenceNumber, newBound);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType") // Passed from other operations returning Optional
    private boolean reuseExtantValue(Optional<CoordinationStoreState<T>> oldState, T targetValue) {
        if (!oldState.isPresent()) {
            return false;
        }
        CoordinationStoreState<T> extantState = oldState.get();
        return extantState
                .value()
                .map(extantValue -> shouldReuseExtantValue.test(extantValue, targetValue))
                .orElse(false);
    }

    private CheckAndSetResult<ValueAndBound<T>> extractRelevantValues(
            T targetValue, long newBound, CheckAndSetResult<SequenceAndBound> casResult) {
        if (casResult.successful()) {
            return CheckAndSetResult.of(true, ImmutableList.of(ValueAndBound.of(Optional.of(targetValue), newBound)));
        }
        return CheckAndSetResult.of(
                false,
                casResult.existingValues().stream()
                        .map(value -> ValueAndBound.of(getValue(value.sequence()), value.bound()))
                        .collect(Collectors.toList()));
    }

    private long getNewBound(long pointInTime) {
        return pointInTime + ADVANCEMENT_QUANTUM;
    }

    @VisibleForTesting
    Optional<T> getValue(long sequenceNumber) {
        Preconditions.checkState(
                sequenceNumber > 0, "Only positive sequence numbers are supported, but found %s", sequenceNumber);
        return readFromCoordinationTable(getCellForSequence(sequenceNumber))
                .map(Value::getContents)
                .map(this::deserializeValue);
    }

    @VisibleForTesting
    void putUnlessValueExists(long sequenceNumber, T value) {
        Preconditions.checkState(
                sequenceNumber > 0, "Only positive sequence numbers are supported, but found %s", sequenceNumber);
        try {
            kvs.putUnlessExists(
                    AtlasDbConstants.COORDINATION_TABLE,
                    ImmutableMap.of(getCellForSequence(sequenceNumber), serializeValue(value)));
        } catch (KeyAlreadyExistsException e) {
            throw new SafeIllegalStateException(
                    "The coordination store failed a putUnlessExists. This is unexpected"
                            + " as it implies timestamps may have been reused, or a writer to the store behaved badly."
                            + " The offending sequence number is logged. "
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
        if (oldValue.map(presentOldValue -> Objects.equals(presentOldValue, newValue))
                .orElse(false)) {
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
                return ImmutableCheckAndSetResult.of(
                        false,
                        e.getActualValues().stream()
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
            log.error(
                    "Error encountered when deserializing {}: {}",
                    SafeArg.of("safeDescriptionOfItemToDeserialize", safeDescriptionOfItemToDeserialize),
                    SafeArg.of("coordinationData", PtBytes.toString(data)),
                    e);
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
            log.error(
                    "Error encountered when serializing {}: {}",
                    SafeArg.of("safeDescriptionOfItemToSerialize", safeDescriptionOfItemToSerialize),
                    SafeArg.of("coordinationData", object),
                    e);
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    Cell getCoordinationValueCell() {
        return getCellForSequence(0);
    }

    @VisibleForTesting
    Cell getCellForSequence(long sequenceNumber) {
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
            throw new SafeIllegalStateException(
                    "Failed to check and set coordination value from oldValue to newValue, but "
                            + "there is no value present in the coordination table",
                    SafeArg.of("oldValue", oldValue),
                    SafeArg.of("newValue", newValue));
        }
        return ImmutableCheckAndSetResult.of(false, ImmutableList.of(actualValue.get()));
    }

    @org.immutables.value.Value.Immutable // We use the AtlasDB Value class too
    interface CoordinationStoreState<T> {
        long sequence();

        long bound();

        Optional<T> value();

        @org.immutables.value.Value.Lazy
        default SequenceAndBound sequenceAndBound() {
            return SequenceAndBound.of(sequence(), bound());
        }

        @org.immutables.value.Value.Lazy
        default ValueAndBound<T> valueAndBound() {
            return ValueAndBound.of(value(), bound());
        }

        static <T> CoordinationStoreState<T> of(long sequence, long bound, Optional<T> value) {
            return ImmutableCoordinationStoreState.<T>builder()
                    .sequence(sequence)
                    .bound(bound)
                    .value(value)
                    .build();
        }
    }
}
