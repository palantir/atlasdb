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
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class LocalDiskSchemaMetadataPersister implements SchemaMetadataPersister {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final File backupFile = new File("var/data/backup"); // TODO(gs): configurable

    @Override
    public void persist(
            Namespace namespace, long timestamp, TimestampType timestampType, InternalSchemaMetadataState state) {
        createNamespaceDirectory(namespace);
        File file = getInternalSchemaMetadataStateFile(namespace, timestampType);
        try {
            OBJECT_MAPPER.writeValue(file, state);
        } catch (IOException e) {
            throw new SafeIllegalStateException(
                    "Unable to write InternalSchemaMetadataState file", e, SafeArg.of("file", file));
        }
    }

    private void createNamespaceDirectory(Namespace namespace) {
        File namespaceDir = getNamespaceDirectory(namespace);
        if (!namespaceDir.exists()) {
            namespaceDir.mkdir();
        }
    }

    private static File getNamespaceDirectory(Namespace namespace) {
        return Paths.get(backupFile.getPath(), namespace.get()).toFile();
    }

    private static File getInternalSchemaMetadataStateFile(Namespace namespace, TimestampType timestampType) {
        return new File(getNamespaceDirectory(namespace), timestampType.filename());
    }
}
