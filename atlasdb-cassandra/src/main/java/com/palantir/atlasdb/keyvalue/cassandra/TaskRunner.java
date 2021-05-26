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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.TaskRunner.CallableWithMetadata.Metadata;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.base.Throwables;
import com.palantir.tracing.DetachedSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import org.immutables.value.Value;

class TaskRunner {
    private final ListeningExecutorService listeningExecutor;

    TaskRunner(ExecutorService executor) {
        this.listeningExecutor = MoreExecutors.listeningDecorator(executor);
    }

    /*
     * Similar to executor.invokeAll, but cancels all remaining tasks if one fails and doesn't spawn new threads if
     * there is only one task
     */
    <V> List<V> runAllTasksCancelOnFailure(List<CallableWithMetadata<V>> tasks) {
        if (tasks.size() == 1) {
            try {
                // Callable<Void> returns null, so can't use immutable list
                return Collections.singletonList(tasks.get(0).task().call());
            } catch (Exception e) {
                throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
            }
        }

        List<ListenableFuture<V>> futures = new ArrayList<>(tasks.size());
        for (CallableWithMetadata<V> task : tasks) {
            DetachedSpan detachedSpan = DetachedSpan.start("task");
            ListenableFuture<V> future = listeningExecutor.submit(task.task());
            futures.add(attachDetachedSpanCompletion(detachedSpan, task.metadata(), future, listeningExecutor));
        }
        try {
            List<V> results = new ArrayList<>(tasks.size());
            for (ListenableFuture<V> future : futures) {
                results.add(future.get());
            }
            return results;
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        } finally {
            for (ListenableFuture<V> future : futures) {
                future.cancel(true);
            }
        }
    }

    private static <V> ListenableFuture<V> attachDetachedSpanCompletion(
            DetachedSpan detachedSpan, Metadata metadata, ListenableFuture<V> future, Executor tracingExecutorService) {
        Futures.addCallback(
                future,
                new FutureCallback<V>() {
                    @Override
                    public void onSuccess(V result) {
                        detachedSpan.complete(metadata.getMetadata());
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        detachedSpan.complete(metadata.getMetadata());
                    }
                },
                tracingExecutorService);
        return future;
    }

    @Value.Immutable
    interface CallableWithMetadata<V> {

        @Value.Immutable
        interface Metadata {

            @Value.Parameter
            String taskName();

            @Value.Parameter
            int numCells();

            @Value.Parameter
            Set<TableReference> tableRefs();

            @Value.Parameter
            String host();

            default Map<String, String> getMetadata() {
                return ImmutableMap.of(
                        "taskName",
                        taskName(),
                        "numCells",
                        String.valueOf(numCells()),
                        "tableRefs",
                        LoggingArgs.tableRefs(tableRefs()).toString(),
                        "host",
                        host());
            }
        }

        @Value.Parameter
        Callable<V> task();

        @Value.Parameter
        Metadata metadata();
    }
}
