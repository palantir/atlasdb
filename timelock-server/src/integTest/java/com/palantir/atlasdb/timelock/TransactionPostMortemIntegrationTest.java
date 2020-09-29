/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.debug.ClientLockDiagnosticCollector;
import com.palantir.atlasdb.debug.ClientLockDiagnosticCollectorImpl;
import com.palantir.atlasdb.debug.FullDiagnosticDigest;
import com.palantir.atlasdb.debug.ImmutableLockDiagnosticComponents;
import com.palantir.atlasdb.debug.ImmutableLockDiagnosticConfig;
import com.palantir.atlasdb.debug.LocalLockTracker;
import com.palantir.atlasdb.debug.LockDiagnosticConfig;
import com.palantir.atlasdb.debug.TransactionPostMortemRunner;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.timelock.TimeLockTestUtils.TransactionManagerContext;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.example.profile.schema.ProfileSchema;
import com.palantir.example.profile.schema.generated.ProfileTableFactory;
import com.palantir.example.profile.schema.generated.UserProfileTable;
import com.palantir.example.profile.schema.generated.UserProfileTable.PhotoStreamId;
import com.palantir.example.profile.schema.generated.UserProfileTable.UserProfileRow;
import com.palantir.paxos.Client;
import com.palantir.refreshable.Refreshable;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;

/**
 * TODO(fdesouza): Remove this once PDS-95791 is resolved.
 * @deprecated Remove this once PDS-95791 is resolved.
 */
@Deprecated
public class TransactionPostMortemIntegrationTest extends AbstractAsyncTimelockServiceIntegrationTest {

    private static final Client TIMELOCK_CLIENT = Client.of("client-with-lock-diagnostics");
    private static final LockDiagnosticConfig LOCK_DIAGNOSTIC_CONFIG = ImmutableLockDiagnosticConfig.builder()
            .ttl(Duration.ofDays(1))
            .maximumSize(Integer.MAX_VALUE)
            .build();
    private static final ImmutableTransactionConfig TRANSACTION_CONFIG = ImmutableTransactionConfig.builder()
            .attachStartTimestampToLockRequestDescriptions(true)
            .build();
    private static final ProfileTableFactory TABLE_FACTORY = ProfileTableFactory.of();
    private static final TableReference TABLE_REFERENCE = TABLE_FACTORY.getUserProfileTable(null).getTableRef();
    private static final int LOCK_TRACKER_SIZE = 10_000;

    private TransactionManagerContext transactionManagerContext;
    private ClientLockDiagnosticCollector diagnosticCollector
            = new ClientLockDiagnosticCollectorImpl(LOCK_DIAGNOSTIC_CONFIG);
    private LocalLockTracker lockTracker = new LocalLockTracker(LOCK_TRACKER_SIZE);
    private TransactionPostMortemRunner runner;

    @Before
    public void setUp() {
        AtlasDbRuntimeConfig runtimeConfig = AtlasDbRuntimeConfig.withSweepDisabled()
                .withTransaction(TRANSACTION_CONFIG);
        transactionManagerContext = TimeLockTestUtils.createTransactionManager(
                cluster,
                TIMELOCK_CLIENT.value(),
                runtimeConfig,
                Optional.of(ImmutableLockDiagnosticComponents.builder()
                        .clientLockDiagnosticCollector(diagnosticCollector)
                        .localLockTracker(lockTracker)
                        .build()),
                ProfileSchema.INSTANCE.getLatestSchema());
        runner = new TransactionPostMortemRunner(
                transactionManagerContext.transactionManager(),
                TABLE_REFERENCE,
                transactionManagerContext.install(),
                Refreshable.only(transactionManagerContext.runtime()),
                diagnosticCollector,
                lockTracker);
    }

    @Test
    public void canRecordTransactionProvenance() throws JsonProcessingException {
        UUID uuid = UUID.randomUUID();
        UserProfileRow row = UserProfileRow.of(uuid);
        runWithRetryVoid(store -> store.putPhotoStreamId(ImmutableMap.of(row, 5L)));
        runWithRetryVoid(store -> store.putPhotoStreamId(ImmutableMap.of(row, 11L)));
        runWithRetryVoid(store -> store.putPhotoStreamId(ImmutableMap.of(row, 19L)));
        runWithRetryVoid(store -> store.putPhotoStreamId(ImmutableMap.of(row, 23L)));
        runWithRetryVoid(store -> store.deletePhotoStreamId(row));

        FullDiagnosticDigest<String> digest = runner.conductPostMortem(row, PhotoStreamId.of(0L).persistColumnName());

        System.out.println(
                ObjectMappers.newServerObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(digest));
    }

    private void runWithRetryVoid(Consumer<UserProfileTable> task) {
        transactionManagerContext.transactionManager().runTaskWithRetry(transaction -> {
            task.accept(TABLE_FACTORY.getUserProfileTable(transaction));
            return null;
        });
    }
}
