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

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableLockWatch.class)
@JsonDeserialize(as = ImmutableLockWatch.class)
public interface LockWatch {
    LockWatch INVALID = ImmutableLockWatch.of(0L, false);

    @Value.Parameter
    long timestamp();

    @Value.Parameter
    boolean fromCommittedTransaction();

    static LockWatch committed(long timestamp) {
        return ImmutableLockWatch.of(timestamp, true);
    }

    static LockWatch uncommitted(long timestamp) {
        return ImmutableLockWatch.of(timestamp, false);
    }

    static LockWatch latest(LockWatch first, LockWatch second) {
        return first.timestamp() > second.timestamp() ? first : second;
    }
}
