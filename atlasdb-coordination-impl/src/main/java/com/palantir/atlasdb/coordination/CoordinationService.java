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

/**
 * A {@link CoordinationService} is used to agree on values being relevant or correct at a given timestamp.
 * The sequence of values being agreed should evolve in a backwards consistent manner; that is, if I read a value
 * for some timestamp TS and make a decision at TS, all future values written to the {@link CoordinationService} should
 * also ensure that no decision that would be inconsistent with the decision made at TS would be made at TS. In general,
 * this means that one should avoid changes that affect behaviour of values below the validity bound.
 *
 * It is the responsibility of the caller to preserve the above property.
 */
public interface CoordinationService<T> {
    /**
     * Returns a value that the coordination service has agreed is appropriate at the provided timestamp.
     *
     * The value returned by this method may change over time. However, values returned by this method would
     * be consistent at this timestamp - that is, the service should never return values for the same timestamp
     * that would be contradictory or result in different operation for uses at this timestamp.
     *
     * This operation will return an empty Optional if no value has been agreed upon yet, or if the current value agreed
     * by the coordination service is not valid at the provided timestamp.
     *
     * @param timestamp timestamp to retrieve the coordinated value for
     * @return value associated with that timestamp
     */
    Optional<ValueAndBound<T>> getValueForTimestamp(long timestamp);

    /**
     * Updates the value that this coordination service stores, marking this value as valid up to a certain timestamp.
     * It may transpire that the value in the coordination service is already acceptable - if so, the provided
     * transformation should simply return the original input. If the transformation returns a value with a bound
     * less than or equal to the bound of the input, the results of the transformation will not be stored.
     *
     * Evolutions of the value must be compatible in terms of backwards consistency as defined in the class docs.
     *
     * The {@link ValueAndBound} returned by the transform must contain a value.
     *
     * @param transform transformation to apply to the existing value and bound the coordination service agrees on
     * @return true if and only if the transformation was applied
     */
    boolean tryTransformCurrentValue(Function<Optional<ValueAndBound<T>>, ValueAndBound<T>> transform);
}
