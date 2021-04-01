/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.timelock.config.TsBoundPersisterConfiguration;
import java.io.IOException;
import java.util.Optional;

public class PersistenceConfigStore {
    private static final BlobStoreUseCase USE_CASE = BlobStoreUseCase.PERSISTENCE_STORAGE;
    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newServerObjectMapper();

    private final SqliteBlobStore sqliteBlobStore;

    public PersistenceConfigStore(SqliteBlobStore sqliteBlobStore) {
        this.sqliteBlobStore = sqliteBlobStore;
    }

    public void storeConfig(TsBoundPersisterConfiguration persisterConfiguration) {
        byte[] configToStore = serializeConfigUnchecked(persisterConfiguration);
        sqliteBlobStore.putValue(USE_CASE, configToStore);
    }

    public Optional<TsBoundPersisterConfiguration> getPersistedConfig() {
        return sqliteBlobStore.getValue(USE_CASE).map(this::deserializeConfigUnchecked);
    }

    private TsBoundPersisterConfiguration deserializeConfigUnchecked(byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, TsBoundPersisterConfiguration.class);
        } catch (IOException e) {
            throw new SafeRuntimeException("Error deserializing previously persisted persister configuration", e);
        }
    }

    private byte[] serializeConfigUnchecked(TsBoundPersisterConfiguration persisterConfiguration) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(persisterConfiguration);
        } catch (JsonProcessingException e) {
            throw new SafeRuntimeException("Error serializing persister configuration", e);
        }
    }
}
