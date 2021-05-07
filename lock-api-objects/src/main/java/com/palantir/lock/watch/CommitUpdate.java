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

package com.palantir.lock.watch;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.CommitUpdate.InvalidateAll;
import com.palantir.lock.watch.CommitUpdate.InvalidateSome;
import java.util.Set;
import org.immutables.value.Value;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = InvalidateAll.class, name = InvalidateAll.TYPE),
    @JsonSubTypes.Type(value = InvalidateSome.class, name = InvalidateSome.TYPE),
})
public interface CommitUpdate {
    <T> T accept(Visitor<T> visitor);

    static CommitUpdate invalidateAll() {
        return ImmutableInvalidateAll.builder().build();
    }

    static CommitUpdate invalidateSome(Set<LockDescriptor> descriptors) {
        return ImmutableInvalidateSome.builder().invalidatedLocks(descriptors).build();
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableInvalidateAll.class)
    @JsonDeserialize(as = ImmutableInvalidateAll.class)
    @JsonTypeName(InvalidateAll.TYPE)
    interface InvalidateAll extends CommitUpdate {
        String TYPE = "invalidateAll";

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.invalidateAll();
        }
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableInvalidateSome.class)
    @JsonDeserialize(as = ImmutableInvalidateSome.class)
    @JsonTypeName(InvalidateSome.TYPE)
    interface InvalidateSome extends CommitUpdate {
        String TYPE = "invalidateSome";

        Set<LockDescriptor> invalidatedLocks();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.invalidateSome(invalidatedLocks());
        }
    }

    interface Visitor<T> {
        T invalidateAll();

        T invalidateSome(Set<LockDescriptor> invalidatedLocks);
    }
}
