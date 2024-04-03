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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class AsyncInitializationState {
    private volatile Runnable cleanupTask = null;
    private final SettableFuture<Void> future = SettableFuture.create();
    private final ListenableFuture<Void> nonCancelPropagating = Futures.nonCancellationPropagating(future);

    synchronized State initToDone() {
        future.set(null);
        return getState();
    }

    synchronized State initToCancelWithCleanupTask(Runnable runnable) {
        if (future.cancel(true)) {
            cleanupTask = runnable;
        }
        return getState();
    }

    ListenableFuture<?> getFuture() {
        return nonCancelPropagating;
    }

    void performCleanupTask() {
        if (cleanupTask != null) {
            cleanupTask.run();
        }
    }

    boolean isDone() {
        return getState() == State.DONE;
    }

    boolean isCancelled() {
        return getState() == State.CANCELLED;
    }

    public enum State {
        INITIALIZING,
        DONE,
        CANCELLED
    }

    private synchronized State getState() {
        if (future.isCancelled()) {
            return State.CANCELLED;
        } else {
            return future.isDone() ? State.DONE : State.INITIALIZING;
        }
    }
}
