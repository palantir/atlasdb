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

import java.util.UUID;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableLockWatchCreatedEvent.class)
@JsonDeserialize(as = ImmutableLockWatchCreatedEvent.class)
@JsonTypeName(LockWatchCreatedEvent.TYPE)
public abstract class LockWatchCreatedEvent implements LockWatchEvent {
    static final String TYPE = "created";

    public abstract UUID lockWatchId();
    public abstract LockWatchRequest request();

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public static LockWatchEvent.Builder builder(LockWatchRequest request, UUID lockWatchId) {
        ImmutableLockWatchCreatedEvent.Builder builder = ImmutableLockWatchCreatedEvent.builder()
                .request(request)
                .lockWatchId(lockWatchId);
        return seq -> builder.sequence(seq).build();
    }
}
