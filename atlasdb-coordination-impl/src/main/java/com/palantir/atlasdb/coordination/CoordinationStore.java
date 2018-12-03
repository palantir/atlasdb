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

package com.palantir.atlasdb.coordination;

import java.util.Optional;
import java.util.function.Function;

import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;

/**
 * A {@link CoordinationStore} stores data that a {@link CoordinationService} may use.
 */
public interface CoordinationStore<T> {
    /**
     * Gets the value stored in this {@link CoordinationStore}. This value may not be the most recent value; however,
     * it is guaranteed that any value returned by this method will be at least as current as any value returned by
     * a call that returned prior to this method being invoked.
     *
     * @return available value and bound; empty if none has ever been stored
     */
    Optional<ValueAndBound<T>> getAgreedValue();

    /**
     * Proposes a new value to be stored in this {@link CoordinationStore} based on applying the transform passed
     * to an existing {@link ValueAndBound}. It is the responsibility of users to confirm whether their transform
     * succeeded or not.
     *
     * @param transform transformation of the original value passed
     * @return a {@link CheckAndSetResult} indicating if the proposal was successful and the current value
     */
    CheckAndSetResult<ValueAndBound<T>> transformAgreedValue(
            Function<ValueAndBound<T>, T> transform);
}
