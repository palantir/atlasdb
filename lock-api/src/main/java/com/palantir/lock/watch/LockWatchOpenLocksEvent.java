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

import java.util.Set;
import java.util.UUID;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.lock.LockDescriptor;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableLockWatchOpenLocksEvent.class)
@JsonDeserialize(as = ImmutableLockWatchOpenLocksEvent.class)
@JsonTypeName(LockWatchOpenLocksEvent.TYPE)
/**
 * LockWatchOpenLocksEvent is a special event capturing the state of all lock descriptors when registering a lock watch.
 */
public abstract class LockWatchOpenLocksEvent implements LockWatchEvent {
    static final String TYPE = "openLocks";

    public abstract UUID lockWatchId();
    public abstract Set<LockDescriptor> lockDescriptors();

    @Override
    public int size() {
        return lockDescriptors().size();
    }

    @Override
    public <T> T accept(LockWatchEvent.Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public static LockWatchEvent.Builder builder(Set<LockDescriptor> lockDescriptors, UUID lockWatchId) {
        ImmutableLockWatchOpenLocksEvent.Builder builder = ImmutableLockWatchOpenLocksEvent.builder()
                .lockDescriptors(lockDescriptors)
                .lockWatchId(lockWatchId);
        return seq -> builder.sequence(seq).build();
    }
}
