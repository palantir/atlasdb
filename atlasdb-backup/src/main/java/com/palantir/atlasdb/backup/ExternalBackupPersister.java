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
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.timelock.api.Namespace;
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
    private static final String SCHEMA_METADATA_FILE_NAME = "schemaMetadata";
    private static final String COMPLETED_BACKUP_FILE_NAME = "completedBackup";

    private final Function<Namespace, Path> pathFactory;

    public ExternalBackupPersister(Function<Namespace, Path> pathFactory) {
        this.pathFactory = pathFactory;
    }

    @Override
    public void storeSchemaMetadata(Namespace namespace, InternalSchemaMetadataState internalSchemaMetadataState) {
        File schemaMetadataFile = getSchemaMetadataFile(namespace);
        writeToFile(namespace, schemaMetadataFile, internalSchemaMetadataState);
    }

    @Override
    public Optional<InternalSchemaMetadataState> getSchemaMetadata(Namespace namespace) {
        return loadFromFile(namespace, getSchemaMetadataFile(namespace), InternalSchemaMetadataState.class);
    }

    @Override
    public void storeCompletedBackup(CompletedBackup completedBackup) {
        Namespace namespace = completedBackup.getNamespace();
        File completedBackupFile = getCompletedBackupFile(namespace);
        writeToFile(namespace, completedBackupFile, completedBackup);
    }

    @Override
    public Optional<CompletedBackup> getCompletedBackup(Namespace namespace) {
        return loadFromFile(namespace, getCompletedBackupFile(namespace), CompletedBackup.class);
    }

    private File getSchemaMetadataFile(Namespace namespace) {
        return getFile(namespace, SCHEMA_METADATA_FILE_NAME);
    }

    private File getCompletedBackupFile(Namespace namespace) {
        return getFile(namespace, COMPLETED_BACKUP_FILE_NAME);
    }

    private File getFile(Namespace namespace, String fileName) {
        return new File(pathFactory.apply(namespace).toFile(), fileName);
    }

    private void writeToFile(Namespace namespace, File file, Object data) {
        try (OutputStream os = Files.newOutputStream(file.toPath())) {
            os.write(OBJECT_MAPPER.writeValueAsBytes(data));
            os.flush();
        } catch (IOException e) {
            log.error(
                    "Failed to store file",
                    SafeArg.of("fileName", file.getName()),
                    SafeArg.of("namespace", namespace),
                    e);
            throw new RuntimeException(e);
        }
    }

    private <T> Optional<T> loadFromFile(Namespace namespace, File file, Class<T> clazz) {
        if (!file.exists()) {
            log.info(
                    "Tried to load file, but it did not exist",
                    SafeArg.of("fileType", file.getName()),
                    SafeArg.of("namespace", namespace));
            return Optional.empty();
        }

        try {
            T state = OBJECT_MAPPER.readValue(file, clazz);
            log.info(
                    "Successfully loaded file",
                    SafeArg.of("fileType", file.getName()),
                    SafeArg.of("namespace", namespace));
            return Optional.of(state);
        } catch (IOException e) {
            log.warn(
                    "Failed to read file",
                    SafeArg.of("fileType", file.getName()),
                    SafeArg.of("namespace", namespace),
                    e);
            return Optional.empty();
        }
    }
}
