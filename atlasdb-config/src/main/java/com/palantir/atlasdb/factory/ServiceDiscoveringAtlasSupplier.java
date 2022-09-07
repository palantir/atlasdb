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
package com.palantir.atlasdb.factory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.DerivedSnapshotConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;
import com.palantir.util.debug.ThreadDumps;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class ServiceDiscoveringAtlasSupplier {
    private static final SafeLogger log = SafeLoggerFactory.get(ServiceDiscoveringAtlasSupplier.class);

    private static String timestampServiceCreationInfo = null;

    private final Optional<LeaderConfig> leaderConfig;
    private final Supplier<KeyValueService> keyValueService;
    private final Supplier<ManagedTimestampService> timestampService;
    private final Supplier<TimestampStoreInvalidator> timestampStoreInvalidator;
    private final DerivedSnapshotConfig derivedSnapshotConfig;
    private final boolean collectThreadDumps;

    public ServiceDiscoveringAtlasSupplier(
            MetricsManager metricsManager,
            KeyValueServiceConfig config,
            Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            Optional<LeaderConfig> leaderConfig,
            Optional<String> namespace,
            Optional<TableReference> tableReferenceOverride,
            boolean initializeAsync,
            boolean collectThreadDumpOnInit,
            LongSupplier timestampSupplier) {
        this.leaderConfig = leaderConfig;
        this.collectThreadDumps = collectThreadDumpOnInit;

        AtlasDbFactory atlasFactory = AtlasDbServiceDiscovery.createAtlasFactoryOfCorrectType(config);
        keyValueService = Suppliers.memoize(() -> atlasFactory.createRawKeyValueService(
                metricsManager, config, runtimeConfig, leaderConfig, namespace, timestampSupplier, initializeAsync));
        timestampService = () -> atlasFactory.createManagedTimestampService(
                getKeyValueService(), tableReferenceOverride, initializeAsync);
        timestampStoreInvalidator =
                () -> atlasFactory.createTimestampStoreInvalidator(getKeyValueService(), tableReferenceOverride);
        derivedSnapshotConfig = atlasFactory.createDerivedSnapshotConfig(config, runtimeConfig.get());
    }

    public KeyValueService getKeyValueService() {
        return keyValueService.get();
    }

    public DerivedSnapshotConfig getDerivedSnapshotConfig() {
        return derivedSnapshotConfig;
    }

    public synchronized ManagedTimestampService getManagedTimestampService() {
        log.info(
                "[timestamp-service-creation] Fetching timestamp service from "
                        + "thread {}. This should only happen once.",
                SafeArg.of("threadName", Thread.currentThread().getName()));

        if (collectThreadDumps) {
            if (timestampServiceCreationInfo == null) {
                timestampServiceCreationInfo = ThreadDumps.programmaticThreadDump();
            } else {
                handleMultipleTimestampFetch();
            }
        }

        return timestampService.get();
    }

    public synchronized TimestampStoreInvalidator getTimestampStoreInvalidator() {
        return timestampStoreInvalidator.get();
    }

    private void handleMultipleTimestampFetch() {
        try {
            String threadDumpFile = saveThreadDumps();
            reportMultipleTimestampFetch(threadDumpFile);
        } catch (IOException e) {
            log.error(
                    "[timestamp-service-creation] The timestamp service was fetched for a second time. "
                            + "We tried to output thread dumps to a temporary file, but encountered an error.",
                    e);
        }
    }

    @VisibleForTesting
    static String saveThreadDumps() throws IOException {
        File file = getTempFile();
        return saveThreadDumpsToFile(file);
    }

    private static File getTempFile() throws IOException {
        String tempDir = System.getProperty("java.io.tmpdir");
        Path path = Paths.get(tempDir, "atlas-timestamp-service-creation.log");
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        return path.toFile();
    }

    private static String saveThreadDumpsToFile(File file) throws IOException {
        try (OutputStream outputStream = new FileOutputStream(file);
                OutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);
                PrintWriter writer = new PrintWriter(bufferedOutputStream, false, StandardCharsets.UTF_8)) {
            writer.println("This file contains thread dumps that will be useful for the AtlasDB Dev team, in case you "
                    + "hit a MultipleRunningTimestampServices error. In this case, please send this file to them.");
            writer.print("First thread dump: ");
            writer.println(timestampServiceCreationInfo);
            writer.print("Second thread dump: ");
            writer.println(ThreadDumps.programmaticThreadDump());
            writer.flush();
            return file.getPath();
        }
    }

    private void reportMultipleTimestampFetch(String path) {
        if (!leaderConfig.isPresent()) {
            log.warn(
                    "[timestamp-service-creation] Timestamp service fetched for a second time, and there is no leader"
                        + " config. This means that you may soon encounter the MultipleRunningTimestampServices error."
                        + " Thread dumps from both fetches of the timestamp service have been outputted to {}. If you"
                        + " encounter a MultipleRunningTimestampServices error, please send this file to support.",
                    UnsafeArg.of("path", path));
        } else {
            log.warn(
                    "[timestamp-service-creation] Timestamp service fetched for a second time. This is only OK if "
                            + "you are running in an HA configuration and have just had a leadership election. "
                            + "You do have a leader config, but we're outputting thread dumps from both fetches of the "
                            + "timestamp service, in case this second service was created in error. "
                            + "Thread dumps from both fetches of the timestamp service have been outputted to {}. "
                            + "If you encounter a MultipleRunningTimestampServices error, please send this file to "
                            + "support.",
                    UnsafeArg.of("path", path));
        }
    }
}
