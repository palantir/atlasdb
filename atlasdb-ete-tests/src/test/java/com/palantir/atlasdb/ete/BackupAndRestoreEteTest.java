/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.ete;

import static com.palantir.conjure.java.api.testing.Assertions.assertThatRemoteExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.backup.BackupAndRestoreResource;
import com.palantir.atlasdb.backup.RestoreRequest;
import com.palantir.atlasdb.backup.RestoreRequestWithId;
import com.palantir.atlasdb.backup.api.AtlasService;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.api.ServiceId;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timestamp.EteTimestampResource;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.conjure.java.api.errors.ErrorType;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import org.awaitility.Awaitility;
import org.junit.Test;

public class BackupAndRestoreEteTest {
    private static final Todo TODO = ImmutableTodo.of("some stuff to do");
    private static final Namespace NAMESPACE = Namespace.of("atlasete");
    private static final AtlasService ATLAS_SERVICE = AtlasService.of(ServiceId.of("a"), NAMESPACE);
    private static final Namespace NAMESPACE_2 = Namespace.of("atlasete-2");
    private static final AtlasService ATLAS_SERVICE_2 = AtlasService.of(ServiceId.of("b"), NAMESPACE_2);
    private static final ImmutableSet<AtlasService> ATLAS_SERVICES = ImmutableSet.of(ATLAS_SERVICE, ATLAS_SERVICE_2);

    private final TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);
    private final BackupAndRestoreResource backupResource =
            EteSetup.createClientToSingleNode(BackupAndRestoreResource.class);
    private final EteTimestampResource timestampClient = EteSetup.createClientToSingleNode(EteTimestampResource.class);

    @Test
    public void canPrepareBackup() {
        addTodo();
        assertThat(backupResource.getStoredImmutableTimestamp(ATLAS_SERVICE)).isEmpty();

        Set<AtlasService> preparedAtlasServices = backupResource.prepareBackup(ATLAS_SERVICES);
        assertThat(preparedAtlasServices).containsExactly(ATLAS_SERVICE, ATLAS_SERVICE_2);

        // verify we persisted the immutable timestamp to disk
        assertThat(backupResource.getStoredImmutableTimestamp(ATLAS_SERVICE)).isNotEmpty();
    }

    @Test
    public void canCompletePreparedBackup() {
        addTodo();
        backupResource.prepareBackup(ATLAS_SERVICES);

        Long immutableTimestamp =
                backupResource.getStoredImmutableTimestamp(ATLAS_SERVICE).orElseThrow();

        assertThat(backupResource.getStoredBackup(ATLAS_SERVICE)).isEmpty();

        Set<AtlasService> completedAtlasServices = backupResource.completeBackup(ATLAS_SERVICES);
        assertThat(completedAtlasServices).containsExactly(ATLAS_SERVICE, ATLAS_SERVICE_2);

        Optional<CompletedBackup> storedBackup = backupResource.getStoredBackup(ATLAS_SERVICE);
        assertThat(storedBackup).isNotEmpty();
        assertThat(storedBackup.get().getImmutableTimestamp()).isEqualTo(immutableTimestamp);
    }

    @Test
    public void canPrepareAndCompleteRestore() {
        addTodo();
        backupResource.prepareBackup(ATLAS_SERVICES);
        backupResource.completeBackup(ATLAS_SERVICES);

        assertThat(timestampClient.getFreshTimestamp()).isGreaterThan(0L);

        String backupId = "backupId";
        RestoreRequest restoreRequest = RestoreRequest.builder()
                .oldAtlasService(ATLAS_SERVICE)
                .newAtlasService(ATLAS_SERVICE)
                .build();
        Set<AtlasService> preparedAtlasServices =
                backupResource.prepareRestore(RestoreRequestWithId.of(restoreRequest, backupId));
        assertThat(preparedAtlasServices).containsExactly(ATLAS_SERVICE);

        // verify TimeLock is disabled
        assertThatRemoteExceptionThrownBy(timestampClient::getFreshTimestamp)
                .isGeneratedFromErrorType(ErrorType.INTERNAL);

        Set<AtlasService> completedAtlasServices =
                backupResource.completeRestore(RestoreRequestWithId.of(restoreRequest, backupId));
        assertThat(completedAtlasServices).containsExactly(ATLAS_SERVICE);

        // verify TimeLock is re-enabled
        assertThat(timestampClient.getFreshTimestamp()).isGreaterThan(0L);
    }

    private void addTodo() {
        Awaitility.await()
                .atMost(Duration.ofSeconds(60L))
                .ignoreExceptions()
                .pollInterval(Duration.ofSeconds(1L))
                .untilAsserted(() -> todoClient.addTodo(TODO));
    }
}
