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
package com.palantir.atlasdb.todo;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.SweepResults;

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
    public List<Todo> getTodoList() {
        return atlas.getTodoList();
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
    public SweepResults sweepSnapshotIndices() {
        return atlas.sweepSnapshotIndices();
    }

    @Override
    public SweepResults sweepSnapshotValues() {
        return atlas.sweepSnapshotValues();
    }
}

