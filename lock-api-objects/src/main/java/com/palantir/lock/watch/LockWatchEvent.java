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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = LockEvent.class, name = LockEvent.TYPE),
    @JsonSubTypes.Type(value = UnlockEvent.class, name = UnlockEvent.TYPE),
    @JsonSubTypes.Type(value = LockWatchCreatedEvent.class, name = LockWatchCreatedEvent.TYPE)
})
public interface LockWatchEvent {
    long sequence();

    int size();

    <T> T accept(Visitor<T> visitor);

    interface Builder {
        LockWatchEvent build(long sequence);
    }

    interface Visitor<T> {
        T visit(LockEvent lockEvent);

        T visit(UnlockEvent unlockEvent);

        T visit(LockWatchCreatedEvent lockWatchCreatedEvent);
    }
}
