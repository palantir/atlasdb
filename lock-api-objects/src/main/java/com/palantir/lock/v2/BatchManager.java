/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.v2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.palantir.logsafe.Preconditions;

// We probably want to do this a bit more cleanly, not using two ExceptionProneRunners, but
// this is just supposed to capture correctness, not elegance.
public final class BatchManager<R> implements AutoCloseable {

    private final Set<R> resources = new LinkedHashSet<>();
    private final Consumer<R> cleaner;
    private final ExceptionProneRunner runner = new ExceptionProneRunner();

    public BatchManager(Consumer<R> cleaner) {
        this.cleaner = cleaner;
    }

    public R safeExecute(Supplier<R> supplier) {
        // so now, we perform the entire block (since it is async and therefore we need to power through them all
        // then, at the end, if we caught any exceptions, we
        R result = runner.supplySafely(supplier);
        if (result != null) {
            resources.add(result);
        }
        // nulls can be returned (I think)
        return result;
    }

    public void trackAll(Collection<R> collection) {
        collection.forEach(value -> Preconditions.checkArgument(value != null, "Null value being tracked"));
        resources.addAll(collection);
    }

    public List<R> getResources() {
        return new ArrayList<>(resources);
    }

    public BatchManager<R> copy() {
        BatchManager<R> newManager = new BatchManager<>(cleaner);
        newManager.trackAll(resources);
        return newManager;
    }

    @Override
    public void close() {
        try {
            // If there are no errors, then this will not throw, and NO CLEANUP WILL BE DONE (which is what we want,
            // because cleanup -> unlocking things).
            runner.close();
        } catch (RuntimeException e) {
            // Otherwise, we now close everything. If something throws mid-way, we capture
            // and then at the end close, which may throw, propagating up (as we expect).
            try (ExceptionProneRunner closer = new ExceptionProneRunner()) {
                // may just be able to use a single consumer for whole batch
                resources.forEach(resource -> closer.runSafely(() -> cleaner.accept(resource)));
            }
        }

    }

    @Value.Immutable
    interface ResourceHandle<R> {
        @Value.Parameter
        @Nullable
        R value();

        @Value.Parameter
        AutoCloseable close();

        static <R> ResourceHandle<R> of(R value, AutoCloseable closeable) {
            return ImmutableResourceHandle.of(value, closeable);
        }
    }
}
