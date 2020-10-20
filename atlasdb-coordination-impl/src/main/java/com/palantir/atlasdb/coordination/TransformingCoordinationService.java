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
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link TransformingCoordinationService} is a {@link CoordinationService} for T2 objects that uses an underlying
 * {@link CoordinationService} for T1 objects.
 */
public class TransformingCoordinationService<T1, T2> implements CoordinationService<T2> {
    private final CoordinationService<T1> delegate;
    private final Function<T1, T2> transformFromUnderlying;
    private final Function<T2, T1> transformToUnderlying;

    public TransformingCoordinationService(
            CoordinationService<T1> delegate,
            Function<T1, T2> transformFromUnderlying,
            Function<T2, T1> transformToUnderlying) {
        this.delegate = delegate;
        this.transformFromUnderlying = transformFromUnderlying;
        this.transformToUnderlying = transformToUnderlying;
    }

    @Override
    public Optional<ValueAndBound<T2>> getValueForTimestamp(long timestamp) {
        return delegate.getValueForTimestamp(timestamp).map(preservingBounds(transformFromUnderlying));
    }

    @Override
    public CheckAndSetResult<ValueAndBound<T2>> tryTransformCurrentValue(Function<ValueAndBound<T2>, T2> valueUpdater) {
        CheckAndSetResult<ValueAndBound<T1>> delegateResult = delegate.tryTransformCurrentValue(
                preservingBounds(transformFromUnderlying).andThen(valueUpdater).andThen(transformToUnderlying));
        return CheckAndSetResult.of(
                delegateResult.successful(),
                delegateResult.existingValues().stream()
                        .map(preservingBounds(transformFromUnderlying))
                        .collect(Collectors.toList()));
    }

    @Override
    public Optional<ValueAndBound<T2>> getLastKnownLocalValue() {
        return delegate.getLastKnownLocalValue().map(preservingBounds(transformFromUnderlying));
    }

    private static <F, T> Function<ValueAndBound<F>, ValueAndBound<T>> preservingBounds(Function<F, T> base) {
        return fromValueAndBound -> ValueAndBound.of(fromValueAndBound.value().map(base), fromValueAndBound.bound());
    }
}
