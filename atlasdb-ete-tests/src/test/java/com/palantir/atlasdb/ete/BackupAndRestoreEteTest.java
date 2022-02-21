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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.backup.BackupAndRestoreResource;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import org.junit.Test;

public class BackupAndRestoreEteTest {
    private static final Todo TODO = ImmutableTodo.of("some stuff to do");
    private static final Namespace NAMESPACE = Namespace.of("atlasete");

    private final TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);
    private final BackupAndRestoreResource backupResource =
            EteSetup.createClientToSingleNode(BackupAndRestoreResource.class);

    @Test
    public void canPrepareBackup() {
        todoClient.addTodo(TODO);
        backupResource.prepareBackup(ImmutableSet.of(NAMESPACE));

        // verify we persisted the immutable timestamp to disk
        assertThat(backupResource.getStoredImmutableTimestamp(NAMESPACE)).isNotEmpty();
    }
}
