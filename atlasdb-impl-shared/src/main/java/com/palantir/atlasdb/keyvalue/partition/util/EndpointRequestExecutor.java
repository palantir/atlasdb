package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
    private static final int MAX_TASKS_PER_ENDPOINT = 32;
    private static final Logger log = LoggerFactory.getLogger(EndpointRequestExecutor.class);

    final ConcurrentMap<Future<?>, KeyValueService> endpointByFuture;
    final ConcurrentHashMultiset<KeyValueService> numberOfTasksByEndpoint;

    private EndpointRequestExecutor() {
        numberOfTasksByEndpoint = ConcurrentHashMultiset.create();
        endpointByFuture = Maps.newConcurrentMap();
    }

    private static final EndpointRequestExecutor instance = new EndpointRequestExecutor();

    // This API is like ExecutionCompletionService but also takes endpoint reference
    // when submitting a new task.
    public interface EndpointRequestCompletionService<FutureReturnType> {
        Future<FutureReturnType> poll();
        Future<FutureReturnType> poll(long timeout, TimeUnit timeUnit) throws InterruptedException;
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
            public Future<FutureReturnType> poll() {
                return registerTaskCompleted(execSvc.poll());
            }

            @Override
            public Future<FutureReturnType> poll(long timeout,
                    TimeUnit timeUnit) throws InterruptedException {
                return registerTaskCompleted(execSvc.poll(timeout, timeUnit));
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
