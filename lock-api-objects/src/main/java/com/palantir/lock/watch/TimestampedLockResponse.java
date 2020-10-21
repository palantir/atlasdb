/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.watch;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.lock.v2.LockResponse;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableTimestampedLockResponse.class)
@JsonDeserialize(as = ImmutableTimestampedLockResponse.class)
public interface TimestampedLockResponse {
    @Value.Parameter
    Optional<Long> timestamp();

    @Value.Parameter
    LockResponse lockResponse();

    default boolean wasSuccessful() {
        return lockResponse().wasSuccessful();
    }

    static TimestampedLockResponse of(Long timestamp, LockResponse lockResponse) {
        return ImmutableTimestampedLockResponse.of(Optional.ofNullable(timestamp), lockResponse);
    }
}
