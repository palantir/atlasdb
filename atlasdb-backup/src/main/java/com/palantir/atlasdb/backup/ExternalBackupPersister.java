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

package com.palantir.atlasdb.backup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.backup.api.AtlasService;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.api.InProgressBackupToken;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;

public class ExternalBackupPersister implements BackupPersister {
    private static final SafeLogger log = SafeLoggerFactory.get(ExternalBackupPersister.class);

    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newClientObjectMapper();
    private static final ObjectMapper LEGACY_OBJECT_MAPPER =
            OBJECT_MAPPER.copy().setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);

    private static final String SCHEMA_METADATA_FILE_NAME = "internal_schema_metadata_state";
    private static final String BACKUP_TIMESTAMP_FILE_NAME = "backup.timestamp";
    private static final String IMMUTABLE_TIMESTAMP_FILE_NAME = "immutable.timestamp";
    private static final String FAST_FORWARD_TIMESTAMP_FILE_NAME = "fast-forward.timestamp";

    private final Function<AtlasService, Path> pathFactory;

    public ExternalBackupPersister(Function<AtlasService, Path> pathFactory) {
        this.pathFactory = pathFactory;
    }

    @Override
    public void storeSchemaMetadata(AtlasService service, InternalSchemaMetadataState internalSchemaMetadataState) {
        File schemaMetadataFile = getSchemaMetadataFile(service);
        if (log.isDebugEnabled()) {
            log.debug(
                    "Storing schema metadata",
                    SafeArg.of("service", service),
                    SafeArg.of("file", schemaMetadataFile.getPath()));
        }
        writeToFile(service, schemaMetadataFile, internalSchemaMetadataState);
    }

    @Override
    public Optional<InternalSchemaMetadataState> getSchemaMetadata(AtlasService atlasService) {
        return loadFromFile(atlasService, getSchemaMetadataFile(atlasService), InternalSchemaMetadataState.class);
    }

    @Override
    public void storeCompletedBackup(AtlasService atlasService, CompletedBackup completedBackup) {
        if (log.isDebugEnabled()) {
            log.debug("Storing completed backup", SafeArg.of("atlasService", atlasService));
        }
        writeToFile(atlasService, getImmutableTimestampFile(atlasService), completedBackup.getImmutableTimestamp());
        writeToFile(atlasService, getBackupTimestampFile(atlasService), completedBackup.getBackupStartTimestamp());
        writeToFile(atlasService, getFastForwardTimestampFile(atlasService), completedBackup.getBackupEndTimestamp());
    }

    @Override
    public Optional<CompletedBackup> getCompletedBackup(AtlasService atlasService) {
        Optional<Long> immutableTimestamp =
                loadFromFile(atlasService, getImmutableTimestampFile(atlasService), Long.class);
        Optional<Long> startTimestamp = loadFromFile(atlasService, getBackupTimestampFile(atlasService), Long.class);
        Optional<Long> endTimestamp = loadFromFile(atlasService, getFastForwardTimestampFile(atlasService), Long.class);
        if (immutableTimestamp.isEmpty() || startTimestamp.isEmpty() || endTimestamp.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(CompletedBackup.builder()
                .namespace(atlasService.getNamespace())
                .immutableTimestamp(immutableTimestamp.get())
                .backupStartTimestamp(startTimestamp.get())
                .backupEndTimestamp(endTimestamp.get())
                .build());
    }

    @Override
    public void storeImmutableTimestamp(AtlasService atlasService, InProgressBackupToken inProgressBackupToken) {
        writeToFile(
                atlasService, getImmutableTimestampFile(atlasService), inProgressBackupToken.getImmutableTimestamp());
    }

    @Override
    public Optional<Long> getImmutableTimestamp(AtlasService atlasService) {
        return loadFromFile(atlasService, getImmutableTimestampFile(atlasService), Long.class);
    }

    private File getSchemaMetadataFile(AtlasService atlasService) {
        return getFile(atlasService, SCHEMA_METADATA_FILE_NAME);
    }

    private File getImmutableTimestampFile(AtlasService atlasService) {
        return getFile(atlasService, IMMUTABLE_TIMESTAMP_FILE_NAME);
    }

    private File getBackupTimestampFile(AtlasService atlasService) {
        return getFile(atlasService, BACKUP_TIMESTAMP_FILE_NAME);
    }

    private File getFastForwardTimestampFile(AtlasService atlasService) {
        return getFile(atlasService, FAST_FORWARD_TIMESTAMP_FILE_NAME);
    }

    private File getFile(AtlasService atlasService, String fileName) {
        return new File(pathFactory.apply(atlasService).toFile(), fileName);
    }

    private void writeToFile(AtlasService atlasService, File file, Object data) {
        try (OutputStream os = Files.newOutputStream(file.toPath())) {
            os.write(OBJECT_MAPPER.writeValueAsBytes(data));
            os.flush();
        } catch (IOException e) {
            log.error(
                    "Failed to store file",
                    SafeArg.of("fileName", file.getName()),
                    SafeArg.of("atlasService", atlasService),
                    e);
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    <T> Optional<T> loadFromFile(AtlasService atlasService, File file, Class<T> clazz) {
        if (!file.exists()) {
            log.info(
                    "Tried to load file, but it did not exist",
                    SafeArg.of("fileName", file.getName()),
                    SafeArg.of("namespace", atlasService.getNamespace()));
            return Optional.empty();
        }

        try {
            T state = tryLoadFromFile(file, clazz);
            log.info(
                    "Successfully loaded file",
                    SafeArg.of("fileName", file.getName()),
                    SafeArg.of("namespace", atlasService.getNamespace()));
            return Optional.of(state);
        } catch (IOException e) {
            log.warn(
                    "Failed to read file",
                    SafeArg.of("fileName", file.getName()),
                    SafeArg.of("atlasService", atlasService),
                    e);
            return Optional.empty();
        }
    }

    private <T> T tryLoadFromFile(File file, Class<T> clazz) throws IOException {
        try {
            return OBJECT_MAPPER.readValue(file, clazz);
        } catch (MismatchedInputException e) {
            log.debug("Using old mapper format", e);
            return LEGACY_OBJECT_MAPPER.readValue(file, clazz);
        }
    }
}
