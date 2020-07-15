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

package com.palantir.common.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

/**
 * A {@link CheckedRejectionExecutorService} is an executor where submitters of tasks MUST be prepared to handle
 * execution being rejected - and we thus throw a checked exception when submitting tasks.
 */
public final class CheckedRejectionExecutorService {
    private final ExecutorService underlying;

    public CheckedRejectionExecutorService(ExecutorService underlying) {
        this.underlying = underlying;
    }

    public void execute(Runnable runnable) throws CheckedRejectedExecutionException {
        try {
            underlying.execute(runnable);
        } catch (RejectedExecutionException ex) {
            throw new CheckedRejectedExecutionException(ex);
        }
    }

    public <T> Future<T> submit(Callable<T> callable) throws CheckedRejectedExecutionException {
        try {
            return underlying.submit(callable);
        } catch (RejectedExecutionException ex) {
            throw new CheckedRejectedExecutionException(ex);
        }
    }

    ExecutorService getUnderlyingExecutor() {
        return underlying;
    }
}
