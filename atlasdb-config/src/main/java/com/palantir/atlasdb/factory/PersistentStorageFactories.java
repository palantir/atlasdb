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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;

public final class PersistentStorageFactories {
    private static final Logger log = LoggerFactory.getLogger(PersistentStorageFactories.class);
    private static final Pattern UUID_PATTERN = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private static final Set<String> SANITIZED_PATHS = new HashSet<>();

    private PersistentStorageFactories() {}

    /**
     * For the given path does the following: 1) it is sanitized only once per VM lifetime 2) if it exists checks that
     * it is a directory, 3) if it is a directory removes all sub-folders whose names are string representation of a
     * UUID.
     *
     * @param storagePath to the proposed storage location
     */
    public static synchronized void sanitizeStoragePath(String storagePath) {
        if (SANITIZED_PATHS.contains(storagePath)) {
            return;
        }

        File storageDirectory = new File(storagePath);
        if (!storageDirectory.exists()) {
            Preconditions.checkState(
                    storageDirectory.mkdir(),
                    "Not able to create a storage directory",
                    SafeArg.of("storageDirectory", storageDirectory.getAbsolutePath()));
            SANITIZED_PATHS.add(storagePath);
            return;
        }

        Preconditions.checkArgument(
                storageDirectory.isDirectory(),
                "Storage path has to point to a directory",
                SafeArg.of("storageDirectory", storageDirectory.getAbsolutePath()));

        List<File> uuidSubDirectories = uuidNamedSubDirectories(
                MoreObjects.firstNonNull(storageDirectory.listFiles(), new File[0]));
        List<File> directoryContentToDelete = limitFoldersToDelete(uuidSubDirectories);
        deleteDirectories(directoryContentToDelete);

        SANITIZED_PATHS.add(storagePath);
    }

    private static List<File> uuidNamedSubDirectories(File[] files) {
        return Arrays.stream(files)
                .filter(PersistentStorageFactories::folderFilter)
                .collect(Collectors.toList());
    }

    private static boolean folderFilter(File file) {
        return file.isDirectory() && (UUID_PATTERN.matcher(file.getName()).matches());
    }

    private static List<File> limitFoldersToDelete(List<File> folders) {
        if (folders.size() > 2) {
            log.warn(
                    "You are trying to delete more that two UUID named folders during persistent storage start-up. "
                            + "Only two will be deleted.",
                    SafeArg.of("number", folders.size()));
        }
        return folders.stream().limit(2).collect(Collectors.toList());
    }

    private static void deleteDirectories(List<File> directoryContentToDelete) {
        for (File directoryToDelete : directoryContentToDelete) {
            try {
                deletePath(directoryToDelete.toPath().toAbsolutePath());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
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
