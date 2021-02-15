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

package com.palantir.async.initializer;

public class AsyncInitializationState {
    private volatile State state = State.INITIALIZING;
    private volatile Runnable cleanupTask = null;

    synchronized State initToDone() {
        if (state == State.INITIALIZING) {
            state = State.DONE;
        }
        return state;
    }

    synchronized State initToCancelWithCleanupTask(Runnable runnable) {
        if (state == State.INITIALIZING) {
            state = State.CANCELLED;
            cleanupTask = runnable;
        }
        return state;
    }

    void performCleanupTask() {
        if (cleanupTask != null) {
            cleanupTask.run();
        }
    }

    boolean isDone() {
        return state == State.DONE;
    }

    boolean isCancelled() {
        return state == State.CANCELLED;
    }

    public enum State {
        INITIALIZING,
        DONE,
        CANCELLED
    }
}
