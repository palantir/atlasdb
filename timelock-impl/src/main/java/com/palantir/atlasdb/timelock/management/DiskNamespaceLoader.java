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

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.timelock.paxos.PaxosUseCase;
import com.palantir.logsafe.SafeArg;

final class DiskNamespaceLoader {
    private static final Logger logger = LoggerFactory.getLogger(DiskNamespaceLoader.class);
    private final Path rootDataDirectory;

    DiskNamespaceLoader(Path rootDataDirectory) {
        this.rootDataDirectory = rootDataDirectory;
    }

    Set<String> getNamespaces() {
        return Arrays.stream(PaxosUseCase.values())
                .filter(useCase -> useCase != PaxosUseCase.LEADER_FOR_ALL_CLIENTS)
                .map(useCase -> useCase.logDirectoryRelativeToDataDirectory(rootDataDirectory))
                .flatMap(DiskNamespaceLoader::getNamespacesFromUseCaseResolvedDirectory)
                .collect(Collectors.toSet());
    }

    private static Stream<String> getNamespacesFromUseCaseResolvedDirectory(Path logDirectory) {
        if (logDirectory == null) {
            return Stream.of();
        }
        File[] directories = logDirectory.toFile().listFiles(File::isDirectory);
        if (directories == null) {
            logger.error("Could not get file for the directory {}", SafeArg.of("dirName", logDirectory));
            throw new IllegalStateException("No namespace exists for the directory : " + logDirectory);
        }
        return Arrays.stream(directories).map(File::getName);
    }
}
