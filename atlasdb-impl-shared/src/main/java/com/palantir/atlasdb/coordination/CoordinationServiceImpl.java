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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
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
 * Coordinates state concerning internal schema versions of AtlasDB.
 *
 * The table stores all of its data on the same row, to allow for atomic operations that mutate multiple values
 * simultaneously in Cassandra KVS. We do not anticipate this to be a problem as the amount of data to achieve
 * coordination over is small.
 *
 * The coordination keys are stored as dynamic columns. Although the AtlasDB API currently does not expose a way of
 * performing a simultaneous atomic update on these, it can be supported by all underlying KVS implementations we are
 * aware of.
 */
public class CoordinationServiceImpl implements CoordinationService {
    private static final Logger log = LoggerFactory.getLogger(CoordinationServiceImpl.class);

    private static final byte[] GLOBAL_ROW_NAME = PtBytes.toBytes("r");

    private final KeyValueService keyValueService;
    private final ObjectMapper objectMapper = ObjectMappers.newServerObjectMapper();

    private CoordinationServiceImpl(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    public CoordinationService create(KeyValueService keyValueService) {
        Preconditions.checkState(keyValueService.supportsCheckAndSet(),
                "Coordination service can only be set up on a KVS supporting check and set.");
        return new CoordinationServiceImpl(keyValueService);
    }

    @Override
    public <T> VersionedMetadata<T> get(String coordinationKey, Class<T> metadataType) {
        Cell cell = createCellFromCoordinationKey(coordinationKey);
        Map<Cell, Value> response = keyValueService.get(
                AtlasDbConstants.COORDINATION_TABLE,
                ImmutableMap.of(cell, Long.MAX_VALUE));
        byte[] metadata = response.get(cell).getContents();

        JavaType versionedMetadataType = getVersionedMetadataType(metadataType);
        try {
            return objectMapper.readValue(metadata, versionedMetadataType);
        } catch (IOException e) {
            log.warn("Could not deserialize versioned metadata: {}", SafeArg.of("metadataBytes", metadata));
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> void putUnlessExists(String coordinationKey, VersionedMetadata<T> desiredValue) {
        keyValueService.putUnlessExists(
                AtlasDbConstants.COORDINATION_TABLE,
                ImmutableMap.of(
                        createCellFromCoordinationKey(coordinationKey),
                        serializeUnchecked(desiredValue)));
    }

    @Override
    public <T> void checkAndSet(String coordinationKey, VersionedMetadata<T> oldValue, VersionedMetadata<T> newValue) {
        keyValueService.checkAndSet(
                CheckAndSetRequest.singleCell(
                        AtlasDbConstants.COORDINATION_TABLE,
                        createCellFromCoordinationKey(coordinationKey),
                        serializeUnchecked(oldValue),
                        serializeUnchecked(newValue)));
    }

    private Cell createCellFromCoordinationKey(String coordinationKey) {
        return Cell.create(GLOBAL_ROW_NAME, PtBytes.toBytes(coordinationKey));
    }

    private <T> JavaType getVersionedMetadataType(Class<T> metadataType) {
        return objectMapper.getTypeFactory().constructParametrizedType(
                VersionedMetadata.class, VersionedMetadata.class, metadataType);
    }

    private <T> byte[] serializeUnchecked(VersionedMetadata<T> versionedMetadata) {
        try {
            objectMapper.writeValueAsBytes(versionedMetadata);
        } catch (JsonProcessingException e) {
            log.warn("Could not serialize versioned metadata: {}", SafeArg.of("versionedMetadata", versionedMetadata));
            throw new RuntimeException(e);
        }
    }
}
