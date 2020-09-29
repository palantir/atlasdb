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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import java.util.Set;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableLockWatchCreatedEvent.class)
@JsonDeserialize(as = ImmutableLockWatchCreatedEvent.class)
@JsonTypeName(LockWatchCreatedEvent.TYPE)
public abstract class LockWatchCreatedEvent implements LockWatchEvent {
    static final String TYPE = "created";

    public abstract Set<LockWatchReference> references();
    public abstract Set<LockDescriptor> lockDescriptors();

    @Override
    public int size() {
        return references().size() + lockDescriptors().size();
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public static LockWatchEvent.Builder builder(Set<LockWatchReference> references, Set<LockDescriptor> descriptors) {
        ImmutableLockWatchCreatedEvent.Builder builder = ImmutableLockWatchCreatedEvent.builder()
                .references(references)
                .lockDescriptors(descriptors);
        return seq -> builder.sequence(seq).build();
    }

    public static LockWatchEvent fromSnapshot(LockWatchStateUpdate.Snapshot snapshot) {
        return builder(snapshot.lockWatches(), snapshot.locked()).build(snapshot.lastKnownVersion());
    }
}
