/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.migration;

import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigs;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.DefaultConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.Visitor;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.workload.config.WorkloadServerInstallConfiguration;
import com.palantir.atlasdb.workload.config.WorkloadServerRuntimeConfiguration;
import com.palantir.atlasdb.workload.migration.cql.CqlSessionProvider;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MigrationRunner {
    private static final SecureRandom SECURE_RANDOM = DefaultNativeSamplingSecureRandomFactory.INSTANCE.create();
    private static final SafeLogger log = SafeLoggerFactory.get(MigrationRunner.class);
    private final MigrationTracker migrationTracker;

    public MigrationRunner(MigrationTracker tracker) {
        this.migrationTracker = tracker;
    }

    public void executeMigration(
            WorkloadServerInstallConfiguration installConfiguration,
            WorkloadServerRuntimeConfiguration runtimeConfiguration) {
        //        disableFaults(); TODO: We could make this configurable.
        run(installConfiguration, runtimeConfiguration);
        //        enableFaults();
    }

    public void scheduleRandomlyInFuture( // TODO: Instead, give this a random chance on happening on each read/write
            ScheduledExecutorService executorService,
            WorkloadServerInstallConfiguration installConfiguration,
            WorkloadServerRuntimeConfiguration runtimeConfiguration) {
        int delay = SECURE_RANDOM.nextInt(300) + 5; // +5 to allow things to start up properly, hacky, although
        // I should trust the fuzzer once I'm running it on Antithesis
        // The above magic numbers are completely arbitrary.
        log.info("Waiting {} seconds before starting migration", SafeArg.of("delay", delay));
        executorService.schedule(
                () -> {
                    try {
                        executeMigration(installConfiguration, runtimeConfiguration);
                    } catch (Exception e) {
                        log.info("Exception when running migration", e);
                        throw e;
                    } finally {
                        migrationTracker.markMigrationAsComplete();
                    }
                },
                delay,
                TimeUnit.SECONDS);
    }

    private void run(
            WorkloadServerInstallConfiguration installConfiguration,
            WorkloadServerRuntimeConfiguration runtimeConfiguration) {
        CassandraKeyValueServiceConfigs config = CassandraKeyValueServiceConfigs.fromKeyValueServiceConfigsOrThrow(
                installConfiguration.atlas().keyValueService(),
                Refreshable.only(runtimeConfiguration.atlas().flatMap(AtlasDbRuntimeConfig::keyValueService)));
        log.info("====STARTING MIGRATION====");
        Set<String> hostnames = config.runtimeConfig().get().servers().accept(new Visitor<>() {
            @Override
            public Set<String> visit(DefaultConfig defaultConfig) {
                throw new SafeRuntimeException("Expecting cql capable hosts");
            }

            @Override
            public Set<String> visit(CqlCapableConfig cqlCapableConfig) {
                return cqlCapableConfig.cqlHosts().stream()
                        .map(InetSocketAddress::getHostName)
                        .collect(Collectors.toSet());
            }
        });
        try (CqlSessionProvider sessionProvider = new CqlSessionProvider(config)) {
            MigratingCassandraCoordinator coordinator =
                    MigratingCassandraCoordinator.create(sessionProvider::getSession, hostnames, migrationTracker);
            log.info("Starting run");
            coordinator.runForward();
            migrationTracker.markMigrationAsComplete();
        }
        log.info("====FINISHED MIGRATION====");
    }

    //    private static void disableFaults() {
    //        log.info("antithesis: stop_faults");
    //    }
    //
    //    private static void enableFaults() {
    //        log.info("antithesis: start_faults");
    //    }
}
