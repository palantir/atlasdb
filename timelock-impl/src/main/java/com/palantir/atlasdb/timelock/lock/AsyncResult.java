/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

public class AsyncResult<T> {

    private final CompletableFuture<T> future;

    public static AsyncResult<Void> completedResult() {
        return new AsyncResult<>(CompletableFuture.completedFuture(null));
    }

    public AsyncResult() {
        this(new CompletableFuture<T>());
    }

    private AsyncResult(CompletableFuture<T> future) {
        this.future = future;
    }

    public void complete(T result) {
        Preconditions.checkState(
                future.complete(result));
    }

    public void fail(Throwable error) {
        Preconditions.checkState(
                future.completeExceptionally(error));
    }

    public void timeout() {
        Preconditions.checkState(
                future.completeExceptionally(new TimeoutException()));
    }

    public boolean isFailed() {
        return future.isCompletedExceptionally();
    }

    public boolean isCompletedSuccessfully() {
        return future.isDone() && !future.isCompletedExceptionally();
    }

    public boolean isComplete() {
        return future.isDone();
    }

    public T get() {
        Preconditions.checkState(isCompletedSuccessfully());
        return future.join();
    }

    public Throwable getError() {
        Preconditions.checkState(isFailed());
        try {
            future.join();
            throw new IllegalStateException("This result is not failed.");
        } catch (CompletionException e) {
            return e.getCause();
        }
    }

    public AsyncResult<T> concatWith(Supplier<AsyncResult<T>> nextResult) {
        return new AsyncResult<T>(future.thenCompose(ignored -> nextResult.get().future));
    }

    public boolean test(Predicate<T> predicateIfCompletedSuccessfully) {
        if (isCompletedSuccessfully()) {
            return predicateIfCompletedSuccessfully.test(get());
        }
        return false;
    }

    public boolean isTimedOut() {
        if (!future.isCompletedExceptionally()) {
            return false;
        }

        try {
            future.join();
            return false;
        } catch (Throwable e) {
            return isTimeout(e);
        }
    }

    public <U> AsyncResult<U> map(Function<T, U> mapper) {
        return new AsyncResult<U>(future.thenApply(mapper));
    }

    public void onError(Consumer<Throwable> errorHandler) {
        future.exceptionally(error -> {
            if (!isTimeout(error)) {
                errorHandler.accept(error);
            }
            return null;
        });
    }

    public void onTimeout(Runnable timeoutHandler) {
        future.exceptionally(error -> {
            if (isTimeout(error)) {
                timeoutHandler.run();
            }
            return null;
        });
    }

    public void onComplete(Runnable completionHandler) {
        future.whenComplete((a, b) -> completionHandler.run());
    }

    private static boolean isTimeout(Throwable ex) {
        return ex.getCause() instanceof TimeoutException;
    }

    static class TimeoutException extends RuntimeException {

    }

}
