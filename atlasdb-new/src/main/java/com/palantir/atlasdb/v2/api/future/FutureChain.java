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

package com.palantir.atlasdb.v2.api.future;

import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import io.vavr.collection.List;

public final class FutureChain<T> {
    private final Executor executor;
    private final ListenableFuture<T> state;
    private final List<Runnable> deferrals;

    private enum Unknown {}

    public FutureChain(Executor executor, T initialState) {
        this(executor, Futures.immediateFuture(initialState));
    }

    public FutureChain(Executor executor, ListenableFuture<T> initialState) {
        this(executor, initialState, List.empty());
    }

    public FutureChain(Executor executor, ListenableFuture<T> state,
            List<Runnable> deferrals) {
        this.executor = executor;
        this.state = state;
        this.deferrals = deferrals;
    }

    public static <T> FutureChain<T> start(Executor executor, ListenableFuture<T> initialState) {
        return new FutureChain<>(executor, initialState);
    }

    public static <T> FutureChain<T> start(Executor executor, T initialState) {
        return new FutureChain<>(executor, initialState);
    }

    public <R> FutureChain<R> alterState(Function<T, R> resultOperator) {
        return new FutureChain<>(
                executor,
                Futures.transform(state, resultOperator::apply, MoreExecutors.directExecutor()),
                deferrals);
    }

    // deferrals made outside of a loop do not apply in the loop
    public FutureChain<T> whileTrue(Predicate<T> stopIfFalse, UnaryOperator<FutureChain<T>> operation) {
        AsyncFunction<T, T> function = state -> operation.apply(new FutureChain<>(executor, state)).done();
        return new FutureChain<>(executor, FutureWhile.whileTrue(executor, state, function, stopIfFalse), deferrals);
    }

    public <R, S> FutureChain<R> then(AsyncFunction<T, S> operation, BiFunction<T, S, R> resultMerger) {
        ListenableFuture<S> parameter = Futures.transformAsync(state, operation, executor);
        ListenableFuture<R> newState = Futures.whenAllSucceed(state, parameter)
                .call(() -> resultMerger.apply(Futures.getUnchecked(state), Futures.getUnchecked(parameter)), executor);
        return new FutureChain<>(executor, newState, deferrals);
    }

    public <S> FutureChain<T> then(AsyncFunction<T, S> operation) {
        ListenableFuture<S> parameter = Futures.transformAsync(state, operation, executor);
        ListenableFuture<T> newState = Futures.whenAllSucceed(parameter)
                .callAsync(() -> state, MoreExecutors.directExecutor());
        return new FutureChain<>(executor, newState, deferrals);
    }

    public FutureChain<T> defer(Consumer<T> deferral) {
        return new FutureChain<>(
                executor,
                state,
                deferrals.prepend(() -> deferral.accept(Futures.getUnchecked(state))));
    }

    public ListenableFuture<T> done() {
        if (deferrals.isEmpty()) {
            return state;
        }
        ListenableFuture<?> executingDeferrals = Futures.whenAllComplete(state)
                .callAsync(() -> Futures.allAsList(deferrals.map(this::submitDeferral)), executor);
        // unsure if should do complete or succeed
        return Futures.whenAllComplete(executingDeferrals).callAsync(() -> state, MoreExecutors.directExecutor());
    }

    // version of done that doesn't give access to the data
    public ListenableFuture<?> maskedDone() {
        return Futures.transform(done(), $ -> null, MoreExecutors.directExecutor());
    }

    private ListenableFuture<Void> submitDeferral(Runnable deferral) {
        return Futures.submitAsync(() -> {
            deferral.run();
            return null;
        }, executor);
    }
}
