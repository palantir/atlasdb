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
import com.palantir.atlasdb.backup.UniqueBackup;
import com.palantir.atlasdb.backup.api.CompletedBackup;
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
    private static final ImmutableSet<Namespace> NAMESPACES = ImmutableSet.of(NAMESPACE);

    private final TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);
    private final BackupAndRestoreResource backupResource =
            EteSetup.createClientToSingleNode(BackupAndRestoreResource.class);
    private final EteTimestampResource timestampClient = EteSetup.createClientToSingleNode(EteTimestampResource.class);

    @Test
    public void canPrepareBackup() {
        addTodo();
        assertThat(backupResource.getStoredImmutableTimestamp(NAMESPACE)).isEmpty();

        Set<Namespace> preparedNamespaces = backupResource.prepareBackup(NAMESPACES);
        assertThat(preparedNamespaces).containsExactly(NAMESPACE);

        // verify we persisted the immutable timestamp to disk
        assertThat(backupResource.getStoredImmutableTimestamp(NAMESPACE)).isNotEmpty();
    }

    @Test
    public void canCompletePreparedBackup() {
        addTodo();
        backupResource.prepareBackup(NAMESPACES);

        Long immutableTimestamp =
                backupResource.getStoredImmutableTimestamp(NAMESPACE).orElseThrow();

        assertThat(backupResource.getStoredBackup(NAMESPACE)).isEmpty();

        Set<Namespace> completedNamespaces = backupResource.completeBackup(NAMESPACES);
        assertThat(completedNamespaces).containsExactly(NAMESPACE);

        Optional<CompletedBackup> storedBackup = backupResource.getStoredBackup(NAMESPACE);
        assertThat(storedBackup).isNotEmpty();
        assertThat(storedBackup.get().getImmutableTimestamp()).isEqualTo(immutableTimestamp);
    }

    @Test
    public void canPrepareAndCompleteRestore() {
        addTodo();
        backupResource.prepareBackup(NAMESPACES);
        backupResource.completeBackup(NAMESPACES);

        assertThat(timestampClient.getFreshTimestamp()).isGreaterThan(0L);

        UniqueBackup uniqueBackup = UniqueBackup.of(NAMESPACES, "backupId");
        Set<Namespace> preparedNamespaces = backupResource.prepareRestore(uniqueBackup);
        assertThat(preparedNamespaces).containsExactly(NAMESPACE);

        // verify TimeLock is disabled
        assertThatRemoteExceptionThrownBy(timestampClient::getFreshTimestamp)
                .isGeneratedFromErrorType(ErrorType.INVALID_ARGUMENT);

        Set<Namespace> completedNamespaces = backupResource.completeRestore(uniqueBackup);
        assertThat(completedNamespaces).containsExactly(NAMESPACE);

        // Verify timelock is re-enabled
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
