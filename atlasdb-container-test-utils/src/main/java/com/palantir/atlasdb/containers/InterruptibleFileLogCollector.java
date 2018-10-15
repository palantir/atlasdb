/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.containers;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.docker.compose.connection.ContainerName;
import com.palantir.docker.compose.execution.DockerCompose;
import com.palantir.docker.compose.logging.FileLogCollector;
import com.palantir.docker.compose.logging.LogCollector;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("SLF4J_ILLEGAL_PASSED_CLASS")
public class InterruptibleFileLogCollector implements LogCollector {
    private static final Logger log = LoggerFactory.getLogger(FileLogCollector.class);

    private static final long STOP_TIMEOUT_IN_MILLIS = 50;

    private final File logDirectory;

    private ExecutorService executor = null;

    public InterruptibleFileLogCollector(File logDirectory) {
        checkArgument(!logDirectory.isFile(), "Log directory cannot be a file");
        if (!logDirectory.exists()) {
            Validate.isTrue(logDirectory.mkdirs(), "Error making log directory: " + logDirectory.getAbsolutePath());
        }
        this.logDirectory = logDirectory;
    }

    public static LogCollector fromPath(String path) {
        return new InterruptibleFileLogCollector(new File(path));
    }

    @Override
    public synchronized void startCollecting(DockerCompose dockerCompose) throws IOException, InterruptedException {
        if (executor != null) {
            throw new RuntimeException("Cannot start collecting the same logs twice");
        }
        List<ContainerName> containerNames = dockerCompose.ps();
        if (containerNames.size() == 0) {
            return;
        }
        executor = Executors.newFixedThreadPool(containerNames.size());
        containerNames.stream()
                .map(ContainerName::semanticName)
                .forEachOrdered(container -> this.collectLogs(container, dockerCompose));
    }

    private void collectLogs(String container, DockerCompose dockerCompose)  {
        executor.submit(() -> {
            File outputFile = new File(logDirectory, container + ".log");
            log.info("Writing logs for container '{}' to '{}'", container, outputFile.getAbsolutePath());
            try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
                dockerCompose.writeLogs(container, outputStream);
            } catch (IOException e) {
                throw new RuntimeException("Error reading log", e);
            }
        });
    }

    @Override
    public synchronized void stopCollecting() throws InterruptedException {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        if (!executor.awaitTermination(STOP_TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS)) {
            log.warn("docker containers were still running when log collection stopped");
            executor.shutdownNow();
        }
    }
}
