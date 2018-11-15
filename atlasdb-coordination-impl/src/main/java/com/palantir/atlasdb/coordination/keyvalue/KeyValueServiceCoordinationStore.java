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
import java.util.Arrays;
import java.util.Optional;
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
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.coordination.CoordinationStore;
import com.palantir.atlasdb.coordination.SequenceAndBound;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.atlasdb.keyvalue.impl.ImmutableCheckAndSetResult;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
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
 */
// TODO (jkong): Coordination stores should be able to clean up old values.
public final class KeyValueServiceCoordinationStore<T> implements CoordinationStore<T> {
    private static final Logger log = LoggerFactory.getLogger(KeyValueServiceCoordinationStore.class);

    private static final TableMetadata COORDINATION_TABLE_METADATA = getCoordinationTableMetadata();

    private static final long ADVANCEMENT_QUANTUM = 5_000_000L;

    private static final String COORDINATION_SEQUENCE_AND_BOUND_DESCRIPTION = "coordination sequence and bound";
    private static final String VALUE_DESCRIPTION = "value for coordination service";

    private final ObjectMapper objectMapper;
    private final KeyValueService kvs;
    private final byte[] coordinationRow;
    private final LongSupplier sequenceNumberSupplier;
    private final Class<T> clazz;

    private KeyValueServiceCoordinationStore(
            ObjectMapper objectMapper,
            KeyValueService kvs,
            byte[] coordinationRow,
            LongSupplier sequenceNumberSupplier,
            Class<T> clazz) {
        this.objectMapper = objectMapper;
        this.kvs = kvs;
        this.coordinationRow = coordinationRow;
        this.sequenceNumberSupplier = sequenceNumberSupplier;
        this.clazz = clazz;
    }

    public static <T> KeyValueServiceCoordinationStore<T> create(
            ObjectMapper objectMapper,
            KeyValueService kvs,
            byte[] coordinationSequence,
            LongSupplier sequenceNumberSupplier,
            Class<T> clazz) {
        KeyValueServiceCoordinationStore<T> coordinationStore
                = new KeyValueServiceCoordinationStore<>(
                        objectMapper, kvs, coordinationSequence, sequenceNumberSupplier, clazz);
        kvs.createTable(AtlasDbConstants.COORDINATION_TABLE, COORDINATION_TABLE_METADATA.persistToBytes());
        return coordinationStore;
    }

    // TODO (jkong): Since apart from the coordination value all other entries are immutable, we can perform
    // some caching.
    @Override
    public Optional<ValueAndBound<T>> getAgreedValue() {
        return getCoordinationValue()
                .map(sequenceAndBound -> ValueAndBound.of(
                        getValue(sequenceAndBound.sequence()), sequenceAndBound.bound()));
    }

    @Override
    public CheckAndSetResult<ValueAndBound<T>> transformAgreedValue(Function<Optional<T>, T> transform) {
        Optional<SequenceAndBound> coordinationValue = getCoordinationValue();
        T targetValue = transform.apply(coordinationValue.flatMap(
                sequenceAndBound -> getValue(sequenceAndBound.sequence())));

        long sequenceNumber = sequenceNumberSupplier.getAsLong();
        putUnlessValueExists(sequenceNumber, targetValue);

        long newBound = getNewBound(sequenceNumber);
        CheckAndSetResult<SequenceAndBound> casResult = checkAndSetCoordinationValue(
                coordinationValue, SequenceAndBound.of(sequenceNumber, newBound));
        return extractRelevantValues(targetValue, newBound, casResult);
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

    private long getNewBound(long sequenceNumber) {
        return sequenceNumber + ADVANCEMENT_QUANTUM;
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

    private Optional<SequenceAndBound> getCoordinationValue() {
        return readFromCoordinationTable(getCoordinationValueCell())
                .map(Value::getContents)
                .map(this::deserializeSequenceAndBound);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @VisibleForTesting
    CheckAndSetResult<SequenceAndBound> checkAndSetCoordinationValue(
            Optional<SequenceAndBound> oldValue, SequenceAndBound newValue) {
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
            return ImmutableCheckAndSetResult.of(false, e.getActualValues()
                    .stream()
                    .map(this::deserializeSequenceAndBound)
                    .collect(Collectors.toList()));
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
                    SafeArg.of("coordinationData", Arrays.toString(data)));
            throw new RuntimeException(e);
        }
    }

    private byte[] serializeSequenceAndBound(SequenceAndBound sequenceAndBound) {
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

    private Cell getCoordinationValueCell() {
        return getCellForSequence(0);
    }

    private Cell getCellForSequence(long sequenceNumber) {
        return Cell.create(coordinationRow, ValueType.VAR_LONG.convertFromJava(sequenceNumber));
    }

    private Optional<Value> readFromCoordinationTable(Cell cell) {
        return Optional.ofNullable(kvs.get(AtlasDbConstants.COORDINATION_TABLE, ImmutableMap.of(cell, Long.MAX_VALUE))
                .get(cell));
    }

    private static TableMetadata getCoordinationTableMetadata() {
        return new TableMetadata(
                NameMetadataDescription.create(ImmutableList.of(
                        new NameComponentDescription.Builder()
                                .componentName("sequence")
                                .type(ValueType.BLOB)
                                .logSafety(TableMetadataPersistence.LogSafety.SAFE)
                                .build())),
                new ColumnMetadataDescription(new DynamicColumnDescription(
                        NameMetadataDescription.create(
                                ImmutableList.of(
                                        new NameComponentDescription.Builder()
                                                .componentName("sequenceNumber")
                                                .logSafety(TableMetadataPersistence.LogSafety.SAFE)
                                                .type(ValueType.VAR_LONG)
                                                .build())),
                        ColumnValueDescription.forType(ValueType.BLOB))),
                ConflictHandler.IGNORE_ALL,
                TableMetadataPersistence.CachePriority.WARM,
                false,
                0,
                false,
                TableMetadataPersistence.SweepStrategy.NOTHING, // we do our own cleanup
                false,
                TableMetadataPersistence.LogSafety.SAFE);
    }
}
