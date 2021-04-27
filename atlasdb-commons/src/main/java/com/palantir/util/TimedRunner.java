/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.util;

import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.Preconditions;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TimedRunner {
    private static final Logger log = LoggerFactory.getLogger(TimedRunner.class);

    private final ExecutorService executor;
    private final Duration timeoutDuration;

    private TimedRunner(ExecutorService executor, Duration timeoutDuration) {
        this.executor = executor;
        this.timeoutDuration = timeoutDuration;
    }

    public static TimedRunner create(Duration timeoutDuration) {
        return new TimedRunner(PTExecutors.newCachedThreadPool("timed-runner"), timeoutDuration);
    }

    public <T> T run(TaskContext<T> taskContext) throws Exception {
        Future<T> future = executor.submit(() -> taskContext.task().call());
        final Exception failure;
        try {
            return future.get(timeoutDuration.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure = e;
        } catch (ExecutionException e) {
            Preconditions.checkState(e.getCause() instanceof Exception, "Execution threw an Error");
            failure = (Exception) e.getCause();
        } catch (TimeoutException e) {
            future.cancel(true);
            failure = e;
        }

        try {
            taskContext.taskFailureHandler().run();
        } catch (Throwable t) {
            log.warn("Shutdown failure handler threw an exception itself!", t);
            failure.addSuppressed(t);
        }
        throw failure;
    }

    @Value.Immutable
    public interface TaskContext<T> {
        /**
         * Executed to shut down an object.
         */
        Callable<T> task();

        /**
         * Executed only if the shutdownCallback times out or otherwise has an exception.
         */
        Runnable taskFailureHandler();

        static <T> TaskContext<T> create(Callable<T> task, Runnable taskFailureHandler) {
            return ImmutableTaskContext.<T>builder()
                    .task(task)
                    .taskFailureHandler(taskFailureHandler)
                    .build();
        }

        static TaskContext<Void> createRunnable(Runnable task, Runnable taskFailureHandler) {
            return ImmutableTaskContext.<Void>builder()
                    .task(() -> {
                        task.run();
                        return null;
                    })
                    .taskFailureHandler(taskFailureHandler)
                    .build();
        }
    }
}
