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
package com.palantir.atlasdb.todo;

import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

public class SimpleTodoResource implements TodoResource {
    private TodoClient atlas;

    public SimpleTodoResource(TodoClient atlas) {
        this.atlas = atlas;
    }

    @Override
    public void addTodo(Todo todo) {
        atlas.addTodo(todo);
    }

    @Override
    public long addTodoWithIdAndReturnTimestamp(long id, Todo todo) {
        return atlas.addTodoWithIdAndReturnTimestamp(id, todo);
    }

    @Override
    public List<Todo> getTodoList() {
        return atlas.getTodoList();
    }

    @Override
    public boolean doesNotExistBeforeTimestamp(long id, long timestamp) {
        return atlas.doesNotExistBeforeTimestamp(id, timestamp);
    }

    @Override
    public void isHealthy() {
        Preconditions.checkState(atlas.getTodoList() != null);
    }

    @Override
    public void storeSnapshot(String snapshot) {
        InputStream snapshotStream = new ByteArrayInputStream(snapshot.getBytes());
        atlas.storeSnapshot(snapshotStream);
    }

    @Override
    public void storeUnmarkedSnapshot(String snapshot) {
        InputStream snapshotStream = new ByteArrayInputStream(snapshot.getBytes());
        atlas.storeUnmarkedSnapshot(snapshotStream);
    }

    @Override
    public void runIterationOfTargetedSweep() {
        atlas.runIterationOfTargetedSweep();
    }

    @Override
    public SweepResults sweepSnapshotIndices() {
        return atlas.sweepSnapshotIndices();
    }

    @Override
    public SweepResults sweepSnapshotValues() {
        return atlas.sweepSnapshotValues();
    }

    @Override
    public long numberOfCellsDeleted(TableReference tableRef) {
        return atlas.numAtlasDeletes(tableRef);
    }

    @Override
    public long numberOfCellsDeletedAndSwept(TableReference tableRef) {
        return atlas.numSweptAtlasDeletes(tableRef);
    }

    @Override
    public void truncate() {
        atlas.truncate();
    }

    @Override
    public long addNamespacedTodoWithIdAndReturnTimestamp(long id, String namespace, Todo todo) {
        return atlas.addNamespacedTodoWithIdAndReturnTimestamp(id, namespace, todo);
    }

    @Override
    public boolean namespacedTodoDoesNotExistBeforeTimestamp(long id, long timestamp, String namespace) {
        return atlas.namespacedTodoDoesNotExistBeforeTimestamp(id, timestamp, namespace);
    }
}
