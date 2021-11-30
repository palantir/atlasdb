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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.backup.CassandraRepairHelper;
import com.palantir.atlasdb.cassandra.backup.LightweightOppTokenRange;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AtlasRestoreService {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasRestoreService.class);

    private final BackupPersister backupPersister;
    private final CassandraRepairHelper cassandraRepairHelper;

    @VisibleForTesting
    AtlasRestoreService(BackupPersister backupPersister, CassandraRepairHelper cassandraRepairHelper) {
        this.backupPersister = backupPersister;
        this.cassandraRepairHelper = cassandraRepairHelper;
    }

    public static AtlasRestoreService create(
            BackupPersister backupPersister,
            MetricsManager metricsManager,
            Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory,
            Function<Namespace, KeyValueService> keyValueServiceFactory) {
        CassandraRepairHelper cassandraRepairHelper =
                new CassandraRepairHelper(metricsManager, keyValueServiceConfigFactory, keyValueServiceFactory);
        return new AtlasRestoreService(backupPersister, cassandraRepairHelper);
    }

    // Returns the set of namespaces for which we successfully repaired internal tables
    public Set<Namespace> repairInternalTables(
            Set<Namespace> namespaces, Consumer<Map<InetSocketAddress, Set<LightweightOppTokenRange>>> repairTable) {
        return namespaces.stream()
                .filter(this::backupExists)
                .peek(namespace -> cassandraRepairHelper.repairInternalTables(namespace, repairTable))
                .collect(Collectors.toSet());
    }

    private boolean backupExists(Namespace namespace) {
        Optional<CompletedBackup> maybeCompletedBackup = backupPersister.getCompletedBackup(namespace);

        if (maybeCompletedBackup.isEmpty()) {
            log.error("Could not restore namespace, as no backup is stored", SafeArg.of("namespace", namespace));
            return false;
        }

        return true;
    }
}
