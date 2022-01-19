/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.pue;

import org.immutables.value.Value;

@Value.Immutable
public interface PutUnlessExistsValue<V> {
    V value();

    PutUnlessExistsState state();

    @Value.Derived
    default boolean isCommitted() {
        return state() == PutUnlessExistsState.COMMITTED;
    }

    static <T> PutUnlessExistsValue<T> committed(T withValue) {
        return ImmutablePutUnlessExistsValue.<T>builder()
                .value(withValue)
                .state(PutUnlessExistsState.COMMITTED)
                .build();
    }

    static <T> PutUnlessExistsValue<T> staging(T withValue) {
        return ImmutablePutUnlessExistsValue.<T>builder()
                .value(withValue)
                .state(PutUnlessExistsState.STAGING)
                .build();
    }
}
