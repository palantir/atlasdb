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

package com.palantir.atlasdb.http.v2;

import com.palantir.logsafe.Preconditions;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
interface ResultOrThrowable {
    boolean isSuccessful();

    Optional<Object> result();

    Optional<Throwable> throwable();

    @Value.Check
    default void exactlyOneSet() {
        Preconditions.checkState((result().isPresent() ^ throwable().isPresent())
                || (isSuccessful() && throwable().isEmpty()));
    }

    static ResultOrThrowable success(Object result) {
        return ImmutableResultOrThrowable.builder()
                .isSuccessful(true)
                .result(Optional.ofNullable(result))
                .build();
    }

    static ResultOrThrowable failure(Throwable throwable) {
        return ImmutableResultOrThrowable.builder()
                .isSuccessful(false)
                .throwable(throwable)
                .build();
    }
}
