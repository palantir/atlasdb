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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.coordination.CoordinationStore;
import com.palantir.atlasdb.coordination.SequenceAndBound;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
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
import com.palantir.remoting3.ext.jackson.ObjectMappers;

import okio.ByteString;

/**
 * An implementation of {@link CoordinationStore} that persists its state in a {@link KeyValueService}.
 *
 * Specifically, a {@link KeyValueServiceCoordinationStore} stores its data in a single row in the
 * {@link AtlasDbConstants#COORDINATION_TABLE}. This specific row is identified by the store's
 * {@link KeyValueServiceCoordinationStore#coordinationSequence}; the reason for doing this is that it allows
 * for multiple distinct sequences to coexist in one table.
 *
 * Within a single identified row, values are written with dynamic column keys as sequence numbers, which are
 * positive longs. Furthermore, a {@link SequenceAndBound} which controls which of the values is considered to be
 * the most recent in the coordination sequence and how long it is valid for is written at dynamic column key zero.
 * This {@link SequenceAndBound} should contain the ID of the value that is considered to be the most current
 * in the sequence.
 */
// TODO (jkong): Coordination stores should be able to clean up old values.
public class KeyValueServiceCoordinationStore implements CoordinationStore {
    private static final Logger log = LoggerFactory.getLogger(KeyValueServiceCoordinationStore.class);

    private static final TableMetadata COORDINATION_TABLE_METADATA = new TableMetadata(
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
                    ColumnValueDescription.forType(ValueType.VAR_LONG))),
            ConflictHandler.IGNORE_ALL,
            TableMetadataPersistence.CachePriority.WARM,
            false,
            0,
            false,
            TableMetadataPersistence.SweepStrategy.NOTHING, // we do our own cleanup
            false,
            TableMetadataPersistence.LogSafety.SAFE);
    private static final long WRITE_TIMESTAMP = 0L;
    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newServerObjectMapper();

    private final KeyValueService kvs;
    private final byte[] coordinationSequence;

    private KeyValueServiceCoordinationStore(KeyValueService kvs, byte[] coordinationSequence) {
        this.kvs = kvs;
        this.coordinationSequence = coordinationSequence;
    }

    public static CoordinationStore create(KeyValueService kvs, byte[] coordinationSequence) {
        CoordinationStore coordinationStore = new KeyValueServiceCoordinationStore(kvs, coordinationSequence);
        kvs.createTable(AtlasDbConstants.COORDINATION_TABLE, COORDINATION_TABLE_METADATA.persistToBytes());
        return coordinationStore;
    }

    @Override
    public Optional<ByteString> getValue(long sequenceNumber) {
        Preconditions.checkState(
                sequenceNumber > 0,
                "Only positive sequence numbers are supported, but found %s",
                sequenceNumber);
        Cell targetCell = getCellForSequence(sequenceNumber);
        Map<Cell, Value> response = kvs.get(AtlasDbConstants.COORDINATION_TABLE,
                ImmutableMap.of(targetCell, Long.MAX_VALUE));
        return Optional.ofNullable(response.get(targetCell)).map(Value::getContents).map(ByteString::of);
    }

    @Override
    public void putValue(long sequenceNumber, ByteString value) {
        Preconditions.checkState(
                sequenceNumber > 0,
                "Only positive sequence numbers are supported, but found %s",
                sequenceNumber);
        kvs.put(AtlasDbConstants.COORDINATION_TABLE,
                ImmutableMap.of(getCellForSequence(sequenceNumber), value.toByteArray()),
                WRITE_TIMESTAMP);
    }

    @Override
    public Optional<SequenceAndBound> getCoordinationValue() {
        Cell targetCell = getCoordinationValueCell();
        Map<Cell, Value> response
                = kvs.get(AtlasDbConstants.COORDINATION_TABLE, ImmutableMap.of(targetCell, Long.MAX_VALUE));
        return Optional.ofNullable(response.get(targetCell))
                .map(Value::getContents)
                .map(KeyValueServiceCoordinationStore::deserializeSequenceAndBound);
    }

    @Override
    public CheckAndSetResult<SequenceAndBound> checkAndSetCoordinationValue(
            Optional<SequenceAndBound> oldValue, SequenceAndBound newValue) {
        CheckAndSetRequest request = oldValue
                .map(value -> CheckAndSetRequest.singleCell(
                        AtlasDbConstants.COORDINATION_TABLE,
                        getCoordinationValueCell(),
                        serializeSequenceAndBound(value),
                        serializeSequenceAndBound(newValue)))
                .orElseGet(() -> CheckAndSetRequest.newCell(
                        AtlasDbConstants.COORDINATION_TABLE,
                        getCoordinationValueCell(),
                        serializeSequenceAndBound(newValue)));

        try {
            kvs.checkAndSet(request);
            return ImmutableCheckAndSetResult.of(true, ImmutableList.of(newValue));
        } catch (CheckAndSetException e) {
            return ImmutableCheckAndSetResult.of(false, e.getActualValues()
                    .stream()
                    .map(KeyValueServiceCoordinationStore::deserializeSequenceAndBound)
                    .collect(Collectors.toList()));
        }
    }

    private static SequenceAndBound deserializeSequenceAndBound(byte[] contents) {
        try {
            return OBJECT_MAPPER.readValue(contents, SequenceAndBound.class);
        } catch (IOException e) {
            log.error("Error encountered when deserializing sequence and bound: {}",
                    SafeArg.of("sequenceAndBound", Arrays.toString(contents)));
            throw new RuntimeException(e);
        }
    }

    private static byte[] serializeSequenceAndBound(SequenceAndBound sequenceAndBound) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(sequenceAndBound);
        } catch (JsonProcessingException e) {
            log.error("Error encountered when serializing sequence and bound: {}",
                    SafeArg.of("sequenceAndBound", sequenceAndBound));
            throw new RuntimeException(e);
        }
    }

    private Cell getCoordinationValueCell() {
        return getCellForSequence(0);
    }

    private Cell getCellForSequence(long sequenceNumber) {
        return Cell.create(coordinationSequence, ValueType.VAR_LONG.convertFromJava(sequenceNumber));
    }
}
