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
import com.palantir.lock.v2.LockToken;
import java.util.Set;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableLockEvent.class)
@JsonDeserialize(as = ImmutableLockEvent.class)
@JsonTypeName(LockEvent.TYPE)
public abstract class LockEvent implements LockWatchEvent {
    static final String TYPE = "lock";

    public abstract Set<LockDescriptor> lockDescriptors();

    public abstract LockToken lockToken();

    @Override
    public int size() {
        return lockDescriptors().size();
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public static LockWatchEvent.Builder builder(Set<LockDescriptor> lockDescriptors, LockToken lockToken) {
        ImmutableLockEvent.Builder builder =
                ImmutableLockEvent.builder().lockDescriptors(lockDescriptors).lockToken(lockToken);
        return seq -> builder.sequence(seq).build();
    }
}
