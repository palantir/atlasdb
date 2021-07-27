/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class PersistentStoragePathSanitizer {
    private static final SafeLogger log = SafeLoggerFactory.get(PersistentStoragePathSanitizer.class);
    private static final String MAGIC_SUFFIX = "atlasdb-persistent-storage";

    private PersistentStoragePathSanitizer() {}

    /**
     * For the given path deletes all files and directories under the folder named {@code MAGIC_SUFFIX}.
     *
     * @param storagePath to the proposed storage location
     */
    public static Path sanitizeStoragePath(String storagePath) {
        File storageDirectory = new File(storagePath, MAGIC_SUFFIX);

        Preconditions.checkArgument(
                storageDirectory.getParentFile().isDirectory(),
                "Storage path has to point to a directory",
                SafeArg.of("storageDirectory", storageDirectory.getParentFile().getAbsolutePath()));

        if (!storageDirectory.exists()) {
            Preconditions.checkState(
                    storageDirectory.mkdir(),
                    "Not able to create a storage directory",
                    SafeArg.of("storageDirectory", storageDirectory.getAbsolutePath()));
            return storageDirectory.toPath().toAbsolutePath();
        }

        Preconditions.checkState(
                storageDirectory.isDirectory(),
                "Dedicated persistent storage path does not point to a directory. "
                        + "Note this is not the same as the provided storage path as we create a special subfolder.",
                SafeArg.of("storageDirectory", storageDirectory.getAbsolutePath()));

        try {
            deletePath(storageDirectory.toPath().toAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return storageDirectory.toPath().toAbsolutePath();
    }

    private static void deletePath(Path absolutePath) throws IOException {
        Preconditions.checkArgument(
                absolutePath.isAbsolute(),
                "Deletion path needs to be absolute",
                SafeArg.of("path", absolutePath.toString()));
        log.info("Deleted folder during sanitization.", SafeArg.of("path", absolutePath));

        try (Stream<Path> stream = Files.walk(absolutePath)) {
            List<Path> sortedPaths = stream.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
            for (Path filePath : sortedPaths) {
                Files.delete(filePath);
            }
        }
    }
}
