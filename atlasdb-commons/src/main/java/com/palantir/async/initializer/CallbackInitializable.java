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

/**
 * An interface simplifying the creation of initialization Callbacks. If a class C requires a resource R to initialize
 * some of its components, but R may not be ready yet at the time of C's creation, C should implement this interface
 * and pass the desired Callback to R. When R is ready, it can then call {@link Callback#runWithRetry(R)} to initialize
 * C.
 */
public interface CallbackInitializable<R> {
    void initialize(R resource);

    /**
     * If {@link #initialize(R)} failing can result in a state where cleanup is necessary, override this method.
     *
     * @param resource the resource used in initialization.
     * @param initFailure the Throwable causing the failure in initialization.
     */
    default void onInitializationFailureCleanup(R resource, Throwable initFailure) {}

    /**
     * Returns a Callback that runs initialize only once. On initialization failure, executes the specified cleanup and
     * then wraps and throws the cause.
     */
    default Callback<R> singleAttemptCallback() {
        return LambdaCallback.singleAttempt(this::initialize, this::onInitializationFailureCleanup);
    }

    /**
     * Returns a Callback that will retry initialization on failure, unless the cleanup task also throws, in which case
     * the exception thrown by the cleanup task is propagated.
     */
    default Callback<R> retryUnlessCleanupThrowsCallback() {
        return LambdaCallback.retrying(this::initialize, this::onInitializationFailureCleanup);
    }
}
