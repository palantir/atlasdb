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

import com.palantir.common.base.Throwables;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Convenience class for creating callbacks using lambda expressions.
 */
public final class LambdaCallback<R> extends Callback<R> {
    private static final BiConsumer<Object, Throwable> FAIL = (ignore, throwable) -> {
        throw Throwables.rewrapAndThrowUncheckedException(throwable);
    };

    private final Consumer<R> initialize;
    private final BiConsumer<R, Throwable> onInitializationFailure;

    private LambdaCallback(Consumer<R> initialize, BiConsumer<R, Throwable> onInitializationFailure) {
        this.initialize = initialize;
        this.onInitializationFailure = onInitializationFailure;
    }

    /**
     * Creates a callback that will attempt to initialize once. If initialize throws, onInitFailureCleanup is called
     * before wrapping and throwing a RuntimeException.
     * @param initialize initialization method.
     * @param onInitFailureCleanup cleanup to be done in case initialization throws. If this method also throws, it will
     * propagate up to the caller.
     * @return the desired Callback object.
     */
    public static <R> Callback<R> singleAttempt(Consumer<R> initialize, BiConsumer<R, Throwable> onInitFailureCleanup) {
        return new LambdaCallback<>(initialize, onInitFailureCleanup.andThen(FAIL));
    }

    /**
     * Creates a callback that will retry initialization until cleanup also throws. If initialize throws,
     * onInitFailureCleanup is called before calling initialize again.
     * @param initialize initialization method.
     * @param onInitFailureCleanup cleanup to be done in case initialization throws. If this method also throws, no more
     * retries will be attempted and it will propagate up to the caller.
     * @return the desired Callback object.
     */
    public static <R> Callback<R> retrying(Consumer<R> initialize, BiConsumer<R, Throwable> onInitFailureCleanup) {
        return new LambdaCallback<>(initialize, onInitFailureCleanup);
    }

    /**
     * Simple version of {@link #singleAttempt(Consumer, BiConsumer)} when no cleanup is necessary on failure.
     * @param initialize initialization method.
     * @return the desired Callback object.
     */
    public static <R> Callback<R> of(Consumer<R> initialize) {
        return singleAttempt(initialize, (no, cleanup) -> { });
    }

    @Override
    public void init(R resource) {
        initialize.accept(resource);
    }

    @Override
    public void cleanup(R resource, Throwable initFailure) {
        onInitializationFailure.accept(resource, initFailure);
    }
}
