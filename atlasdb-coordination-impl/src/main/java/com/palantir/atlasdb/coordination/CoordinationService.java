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

import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.atlasdb.metrics.Timed;
import java.util.Optional;
import java.util.function.Function;

/**
 * A {@link CoordinationService} is used to agree on values being relevant or correct at a given timestamp.
 * The sequence of values being agreed should evolve in a backwards consistent manner. This means that one should
 * avoid changes that may affect behaviour for decisions taken at timestamps below the validity bound.
 *
 * More formally, suppose we read a value for some timestamp TS. (In relation to AtlasDB transactions, this may be
 * a start or commit timestamp, as long as we are consistent.) All future values written to the
 * {@link CoordinationService} must then ensure that decisions made at TS would be done in a way consistent with our
 * initial read.
 *
 * It is the responsibility of the caller to provide transforms that preserve the above property; the addition of a
 * value at a given timestamp implies that it is OK for readers at all previous timestamps to read that value.
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
    @Timed
    Optional<ValueAndBound<T>> getValueForTimestamp(long timestamp);

    /**
     * Attempts to update the value stored in the {@link CoordinationService} by applying the provided transform
     * to the existing {@link ValueAndBound} that is stored in this coordination service.
     *
     * Evolutions of the value must be compatible in terms of backwards consistency as defined in the class docs.
     *
     * Note that in case of check and set failure, there is no atomicity guarantee between the check and set operation
     * and the returned value and bound; i.e., the returned value and bound may be more recent than the values found
     * when CAS failed.
     *
     * @param transform transformation to apply to the existing value and bound the coordination service agrees on
     * @return a {@link CheckAndSetResult} indicating whether the transform was applied and the current value
     *
     * @throws IllegalStateException if check and set fails, but no current value exists
     */
    @Timed
    CheckAndSetResult<ValueAndBound<T>> tryTransformCurrentValue(Function<ValueAndBound<T>, T> transform);

    /**
     * Returns the most recent value that this coordination service knows about locally.
     *
     * This method should be cheap to compute, and is not expected to make calls to remote services.
     * Consequently, it is possible that the value returned is not actually the last known local value globally
     * (or, for that matter, it is possible that it bears little or no degree of recency).
     *
     * This should only be used for metrics and monitoring of the system; product decisions must not be taken based on
     * any values this returns.
     *
     * @return the last locally known value and bound agreed by this coordination service
     */
    Optional<ValueAndBound<T>> getLastKnownLocalValue();
}
