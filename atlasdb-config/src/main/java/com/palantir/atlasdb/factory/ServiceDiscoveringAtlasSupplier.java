/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.factory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.qos.FakeQosClient;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;
import com.palantir.util.debug.ThreadDumps;

public class ServiceDiscoveringAtlasSupplier {
    private static final Logger log = LoggerFactory.getLogger(ServiceDiscoveringAtlasSupplier.class);
    private static final ServiceLoader<AtlasDbFactory> loader = ServiceLoader.load(AtlasDbFactory.class);

    private static String timestampServiceCreationInfo = null;

    private final KeyValueServiceConfig config;
    private final Optional<LeaderConfig> leaderConfig;
    private final Supplier<KeyValueService> keyValueService;
    private final Supplier<TimestampService> timestampService;
    private final Function<TimestampService, TimestampManagementService> timestampManagementAdapter;
    private final Supplier<TimestampStoreInvalidator> timestampStoreInvalidator;

    public ServiceDiscoveringAtlasSupplier(
            MetricsManager metricsManager, KeyValueServiceConfig config, Optional<LeaderConfig> leaderConfig) {
        this(metricsManager,
                config,
                Optional::empty,
                leaderConfig,
                Optional.empty(),
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC,
                FakeQosClient.INSTANCE);
    }

    public ServiceDiscoveringAtlasSupplier(
            MetricsManager metricsManager,
            KeyValueServiceConfig config,
            Optional<LeaderConfig> leaderConfig,
            Optional<String> namespace,
            Optional<TableReference> timestampTable) {
        this(metricsManager,
                config,
                Optional::empty,
                leaderConfig,
                namespace,
                timestampTable,
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC,
                FakeQosClient.INSTANCE,
                AtlasDbFactory.THROWING_FRESH_TIMESTAMP_SOURCE);
    }

    public ServiceDiscoveringAtlasSupplier(
            MetricsManager metricsManager,
            KeyValueServiceConfig config,
            java.util.function.Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            Optional<LeaderConfig> leaderConfig,
            Optional<String> namespace,
            boolean initializeAsync,
            QosClient qosClient) {
        this(metricsManager,
                config,
                runtimeConfig,
                leaderConfig,
                namespace,
                Optional.empty(),
                initializeAsync,
                qosClient,
                AtlasDbFactory.THROWING_FRESH_TIMESTAMP_SOURCE);
    }

    public ServiceDiscoveringAtlasSupplier(
            MetricsManager metricsManager,
            KeyValueServiceConfig config,
            java.util.function.Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            Optional<LeaderConfig> leaderConfig,
            Optional<String> namespace,
            Optional<TableReference> timestampTable,
            boolean initializeAsync,
            QosClient qosClient,
            LongSupplier timestampSupplier) {
        // TODO (jkong): Remove some duplication between the above constructor and this
        this.config = config;
        this.leaderConfig = leaderConfig;

        AtlasDbFactory atlasFactory = createAtlasFactoryOfCorrectType(config);
        keyValueService = Suppliers.memoize(
                () -> atlasFactory.createRawKeyValueService(
                        metricsManager,
                        config,
                        runtimeConfig,
                        leaderConfig,
                        namespace,
                        timestampSupplier,
                        initializeAsync,
                        qosClient));
        timestampService = () ->
                atlasFactory.createTimestampService(getKeyValueService(), timestampTable, initializeAsync);
        timestampStoreInvalidator = () -> atlasFactory.createTimestampStoreInvalidator(getKeyValueService());
        timestampManagementAdapter = atlasFactory::getTimestampManagementService;
    }

    public KeyValueService getKeyValueService() {
        return keyValueService.get();
    }

    public synchronized TimestampService getTimestampService() {
        log.info("[timestamp-service-creation] Fetching timestamp service from "
                        + "thread {}. This should only happen once.", Thread.currentThread().getName());

        if (timestampServiceCreationInfo == null) {
            timestampServiceCreationInfo = ThreadDumps.programmaticThreadDump();
        } else {
            handleMultipleTimestampFetch();
        }

        return timestampService.get();
    }

    public synchronized TimestampManagementService getTimestampManagementService(TimestampService timestampService) {
        return timestampManagementAdapter.apply(timestampService);
    }

    public synchronized TimestampStoreInvalidator getTimestampStoreInvalidator() {
        return timestampStoreInvalidator.get();
    }

    private void handleMultipleTimestampFetch() {
        try {
            String threadDumpFile = saveThreadDumps();
            reportMultipleTimestampFetch(threadDumpFile);
        } catch (IOException e) {
            log.error("[timestamp-service-creation] The timestamp service was fetched for a second time. "
                    + "We tried to output thread dumps to a temporary file, but encountered an error.", e);
        }
    }

    private AtlasDbFactory createAtlasFactoryOfCorrectType(KeyValueServiceConfig kvsConfig) {
        return StreamSupport.stream(loader.spliterator(), false)
                .filter(producesCorrectType())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No atlas provider for KeyValueService type " + kvsConfig.type() + " could be found."
                                + " Have you annotated it with @AutoService(AtlasDbFactory.class)?"
                ));
    }

    @VisibleForTesting
    String saveThreadDumps() throws IOException {
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

    private String saveThreadDumpsToFile(File file) throws IOException {
        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            writeStringToStream(outputStream,
                    "This file contains thread dumps that will be useful for the AtlasDB Dev team, in case you hit a "
                            + "MultipleRunningTimestampServices error. In this case, please send this file to them.\n");
            writeStringToStream(outputStream, "First thread dump: " + timestampServiceCreationInfo + "\n");
            writeStringToStream(outputStream, "Second thread dump: " + ThreadDumps.programmaticThreadDump() + "\n");
            return file.getPath();
        }
    }

    private void reportMultipleTimestampFetch(String path) {
        if (!leaderConfig.isPresent()) {
            log.warn("[timestamp-service-creation] Timestamp service fetched for a second time, and there is no leader "
                    + "config. This means that you may soon encounter the MultipleRunningTimestampServices error. "
                    + "Thread dumps from both fetches of the timestamp service have been outputted to {}. "
                    + "If you encounter a MultipleRunningTimestampServices error, please send this file to "
                    + "support.", path);
        } else {
            log.warn("[timestamp-service-creation] Timestamp service fetched for a second time. This is only OK if "
                    + "you are running in an HA configuration and have just had a leadership election. "
                    + "You do have a leader config, but we're outputting thread dumps from both fetches of the "
                    + "timestamp service, in case this second service was created in error. "
                    + "Thread dumps from both fetches of the timestamp service have been outputted to {}. "
                    + "If you encounter a MultipleRunningTimestampServices error, please send this file to "
                    + "support.", path);
        }
    }

    private void writeStringToStream(FileOutputStream outputStream, String stringToWrite) throws IOException {
        outputStream.write(stringToWrite.getBytes(StandardCharsets.UTF_8));
    }

    private Predicate<AtlasDbFactory> producesCorrectType() {
        return factory -> config.type().equalsIgnoreCase(factory.getType());
    }
}
