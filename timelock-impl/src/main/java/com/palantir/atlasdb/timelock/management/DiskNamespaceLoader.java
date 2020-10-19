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

package com.palantir.atlasdb.timelock.management;

import com.palantir.atlasdb.timelock.paxos.PaxosUseCase;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.Client;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DiskNamespaceLoader implements PersistentNamespaceLoader {
    private static final Logger log = LoggerFactory.getLogger(DiskNamespaceLoader.class);
    private final Path rootDataDirectory;

    public DiskNamespaceLoader(Path rootDataDirectory) {
        this.rootDataDirectory = rootDataDirectory;
    }

    @Override
    public Set<Client> getAllPersistedNamespaces() {
        return Arrays.stream(PaxosUseCase.values())
                .filter(useCase -> useCase != PaxosUseCase.LEADER_FOR_ALL_CLIENTS)
                .map(useCase -> useCase.logDirectoryRelativeToDataDirectory(rootDataDirectory))
                .flatMap(DiskNamespaceLoader::getNamespacesFromUseCaseResolvedDirectory)
                .map(Client::of)
                .collect(Collectors.toSet());
    }

    private static Stream<String> getNamespacesFromUseCaseResolvedDirectory(Path logDirectory) {
        if (Files.notExists(logDirectory)) {
            log.info("No namespace directory exists at path: {}", SafeArg.of("dirName", logDirectory));
            return Stream.of();
        }
        File[] directories = logDirectory.toFile().listFiles(File::isDirectory);
        if (directories == null) {
            log.error("Namespace(s) cannot be read from directory: {}."
                    + " Either the path does not denote a directory or an I/O error has occurred.",
                    SafeArg.of("dirName", logDirectory));
            throw new IllegalStateException("Failed to read directory: " + logDirectory +
                    ". Either the path is invalid or an I/O error has occurred.");
        }
        return Arrays.stream(directories).map(File::getName);
    }
}
