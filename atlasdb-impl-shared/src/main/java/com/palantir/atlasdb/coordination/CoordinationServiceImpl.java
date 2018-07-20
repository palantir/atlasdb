/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.coordination;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.logsafe.SafeArg;
import com.palantir.remoting3.ext.jackson.ObjectMappers;

/**
 * Coordinates state concerning internal schema versions and metadata used by AtlasDB.
 *
 * The table stores all of its data on the same row, to allow for atomic operations that mutate multiple values
 * simultaneously in Cassandra KVS. We do not anticipate this to be a problem as the amount of data to achieve
 * coordination over is small.
 *
 * The coordination keys are stored as dynamic columns. Although the AtlasDB API currently does not expose a way of
 * performing a simultaneous atomic update on these, it can be supported by all underlying KVS implementations we are
 * aware of.
 */
public class CoordinationServiceImpl<T> implements CoordinationService<T> {
    private static final Logger log = LoggerFactory.getLogger(CoordinationServiceImpl.class);

    private static final byte[] GLOBAL_ROW_NAME = PtBytes.toBytes("r");

    private final KeyValueService keyValueService;
    private final ObjectMapper objectMapper = ObjectMappers.newServerObjectMapper();
    private final Cell synchronizationCell;
    private final Class<T> metadataType;

    private CoordinationServiceImpl(KeyValueService keyValueService, Cell synchronizationCell, Class<T> metadataType) {
        this.keyValueService = keyValueService;
        this.synchronizationCell = synchronizationCell;
        this.metadataType = metadataType;
    }

    public static <T> CoordinationService<T> create(
            KeyValueService keyValueService,
            String coordinationKey,
            Class<T> metadataType) {
        Preconditions.checkState(keyValueService.supportsCheckAndSet(),
                "Coordination service can only be set up on a KVS supporting check and set.");
        return new CoordinationServiceImpl<>(
                keyValueService,
                createCellFromCoordinationKey(coordinationKey),
                metadataType);
    }

    @Override
    public Optional<T> get() {
        Map<Cell, Value> response = keyValueService.get(
                AtlasDbConstants.COORDINATION_TABLE,
                ImmutableMap.of(synchronizationCell, Long.MAX_VALUE));
        return Optional.ofNullable(response.get(synchronizationCell))
                .map(Value::getContents)
                .map(this::deserializeUnchecked);
    }

    @Override
    public void putUnlessExists(T desiredValue) {
        keyValueService.putUnlessExists(
                AtlasDbConstants.COORDINATION_TABLE,
                ImmutableMap.of(synchronizationCell, serializeUnchecked(desiredValue)));
    }

    @Override
    public void checkAndSet(T oldValue, T newValue) {
        keyValueService.checkAndSet(
                CheckAndSetRequest.singleCell(
                        AtlasDbConstants.COORDINATION_TABLE,
                        synchronizationCell,
                        serializeUnchecked(oldValue),
                        serializeUnchecked(newValue)));
    }

    private static Cell createCellFromCoordinationKey(String coordinationKey) {
        return Cell.create(GLOBAL_ROW_NAME, PtBytes.toBytes(coordinationKey));
    }

    private T deserializeUnchecked(byte[] metadata) {
        try {
            return objectMapper.readValue(metadata, metadataType);
        } catch (IOException e) {
            log.warn("Could not deserialize metadata: {}", SafeArg.of("metadataBytes", metadata));
            throw new RuntimeException(e);
        }
    }

    private byte[] serializeUnchecked(T object) {
        try {
            return objectMapper.writeValueAsBytes(object);
        } catch (JsonProcessingException e) {
            log.warn("Could not serialize metadata: {}", SafeArg.of("object", object));
            throw new RuntimeException(e);
        }
    }
}
