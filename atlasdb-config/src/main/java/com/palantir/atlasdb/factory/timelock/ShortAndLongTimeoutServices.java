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

package com.palantir.atlasdb.factory.timelock;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.immutables.value.Value;

import com.palantir.atlasdb.factory.ServiceCreator;

@Value.Immutable
public interface ShortAndLongTimeoutServices<T> {
    T longTimeout();
    T shortTimeout();

    static <T> ShortAndLongTimeoutServices<T> create(ServiceCreator serviceCreator, Class<T> clazz) {
        return ImmutableShortAndLongTimeoutServices.<T>builder()
                .longTimeout(serviceCreator.createService(clazz))
                .shortTimeout(serviceCreator.createServiceWithShortTimeout(clazz))
                .build();
    }

    default <U> ShortAndLongTimeoutServices<U> map(Function<T, U> mapper) {
        return ImmutableShortAndLongTimeoutServices.<U>builder()
                .longTimeout(mapper.apply(longTimeout()))
                .shortTimeout(mapper.apply(shortTimeout()))
                .build();
    }

    default <U, V> ShortAndLongTimeoutServices<V> zipWith(ShortAndLongTimeoutServices<U> otherServices,
            BiFunction<T, U, V> combiner) {
        return ImmutableShortAndLongTimeoutServices.<V>builder()
                .longTimeout(combiner.apply(longTimeout(), otherServices.longTimeout()))
                .shortTimeout(combiner.apply(shortTimeout(), otherServices.shortTimeout()))
                .build();
    }
}
