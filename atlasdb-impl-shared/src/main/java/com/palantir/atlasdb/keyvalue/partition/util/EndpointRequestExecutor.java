/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

@ThreadSafe public class EndpointRequestExecutor {
    // This is a SOFT limit and might be sometimes exceeded due to
    // race conditions.
    public static final int MAX_TASKS_PER_ENDPOINT = 32;
    private static final Logger log = LoggerFactory.getLogger(EndpointRequestExecutor.class);

    private final ConcurrentMap<Future<?>, KeyValueService> endpointByFuture;
    private final ConcurrentHashMultiset<KeyValueService> numberOfTasksByEndpoint;

    private EndpointRequestExecutor() {
        numberOfTasksByEndpoint = ConcurrentHashMultiset.create();
        endpointByFuture = Maps.newConcurrentMap();
    }

    private static final EndpointRequestExecutor instance = new EndpointRequestExecutor();

    // This API is like ExecutionCompletionService but also takes endpoint reference
    // when submitting a new task.
    public interface EndpointRequestCompletionService<FutureReturnType> {
        Future<FutureReturnType> submit(Callable<FutureReturnType> callable, KeyValueService kvs);
        Future<FutureReturnType> take() throws InterruptedException;
    }

    public static <FutureReturnType> EndpointRequestCompletionService<FutureReturnType> newService(final ExecutorService executor) {
        return instance.newServiceInternal(executor);
    }

    private <FutureReturnType> EndpointRequestCompletionService<FutureReturnType> newServiceInternal(final ExecutorService executor) {
        return new EndpointRequestCompletionService<FutureReturnType>() {
            final ExecutorCompletionService<FutureReturnType> execSvc = new ExecutorCompletionService<>(executor);

            private <T> Future<T> registerTaskCompleted(Future<T> future) {
                if (future != null) {
                    KeyValueService kvs = Preconditions.checkNotNull(endpointByFuture.get(future));
                    endpointByFuture.remove(future);
                    numberOfTasksByEndpoint.remove(kvs);
                }
                return future;
            }

            @Override
            public Future<FutureReturnType> submit(
                    Callable<FutureReturnType> callable,
                    KeyValueService kvs) {

                final Future<FutureReturnType> ret;

                // Try not to exceed the MAX_TASKS_PER_ENDPOINT number.
                //
                // Note that if multiple threads submit tasks, more than
                // MAX_TASKS_PER_ENDPOINT tasks can be potentially accepted
                // (check-then-act effect).
                //
                // But we prefer to accept more than the limit than to
                // reject more than necessary.
                if (numberOfTasksByEndpoint.count(kvs) > MAX_TASKS_PER_ENDPOINT) {
                    log.warn("Dropping task " + callable + " for kv service " + kvs + " due to queue overflow.");
                    ret = execSvc.submit(new Callable<FutureReturnType>() {
                        @Override
                        public FutureReturnType call() throws Exception {
                            throw new RuntimeException("This task has not been enqueued due to queue overflow!");
                        }
                    });
                } else {
                    numberOfTasksByEndpoint.add(kvs);
                    ret = execSvc.submit(callable);
                }
                endpointByFuture.put(ret, kvs);
                return ret;
            }

            @Override
            public Future<FutureReturnType> take() throws InterruptedException {
                return registerTaskCompleted(execSvc.take());
            }
        };
    }
}
