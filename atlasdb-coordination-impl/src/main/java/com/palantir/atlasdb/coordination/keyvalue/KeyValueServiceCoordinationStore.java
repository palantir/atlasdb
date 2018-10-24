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
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    public KeyValueServiceCoordinationStore(KeyValueService kvs, byte[] coordinationSequence) {
        this.kvs = kvs;
        kvs.createTable(AtlasDbConstants.COORDINATION_TABLE, COORDINATION_TABLE_METADATA.persistToBytes());

        this.coordinationSequence = coordinationSequence;
    }

    @Override
    public Optional<byte[]> getValue(long sequenceNumber) {
        Preconditions.checkState(
                sequenceNumber > 0,
                "Only positive sequence numbers are supported, but found %s",
                sequenceNumber);
        Cell targetCell = getCellForSequence(sequenceNumber);
        Map<Cell, Value> response = kvs.get(AtlasDbConstants.COORDINATION_TABLE,
                ImmutableMap.of(targetCell, Long.MAX_VALUE));
        return Optional.ofNullable(response.get(targetCell)).map(Value::getContents);
    }

    @Override
    public void putValue(long sequenceNumber, byte[] value) {
        Preconditions.checkState(
                sequenceNumber > 0,
                "Only positive sequence numbers are supported, but found %s",
                sequenceNumber);
        kvs.put(AtlasDbConstants.COORDINATION_TABLE,
                ImmutableMap.of(getCellForSequence(sequenceNumber), value),
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
    public Optional<SequenceAndBound> checkAndSetCoordinationValue(
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
            // success
            return Optional.of(newValue);
        } catch (CheckAndSetException e) {
            List<byte[]> actualValues = e.getActualValues();
            Preconditions.checkState(actualValues.size() < 2,
                    "Should not have more than one value, given that our CAS request was on a single cell!");
            return actualValues.stream()
                    .findFirst()
                    .map(KeyValueServiceCoordinationStore::deserializeSequenceAndBound);
        }
    }

    private static SequenceAndBound deserializeSequenceAndBound(byte[] contents) {
        try {
            return OBJECT_MAPPER.readValue(contents, SequenceAndBound.class);
        } catch (IOException e) {
            log.error("Error encountered when deserializing sequence and bound: %s",
                    SafeArg.of("sequenceAndBound", Arrays.toString(contents)));
            throw new RuntimeException(e);
        }
    }

    private static byte[] serializeSequenceAndBound(SequenceAndBound sequenceAndBound) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(sequenceAndBound);
        } catch (JsonProcessingException e) {
            log.error("Error encountered when serializing sequence and bound: %s",
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
