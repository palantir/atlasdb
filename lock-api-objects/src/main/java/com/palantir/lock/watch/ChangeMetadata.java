/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.lock.watch.ChangeMetadata.Created;
import com.palantir.lock.watch.ChangeMetadata.Deleted;
import com.palantir.lock.watch.ChangeMetadata.Unchanged;
import com.palantir.lock.watch.ChangeMetadata.Updated;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

/**
 * The subtypes of this class can be used to document CRUD-like operations that were applied to a value.
 * However, the READ case is not accounted for as no locks are taken out for reads.
 * Every instance should be associated with a {@link com.palantir.lock.LockDescriptor}.
 * It is up to the users of metadata to know the type of value it refers to
 * and how to interpret the different subtypes.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = Unchanged.class, name = Unchanged.TYPE),
    @JsonSubTypes.Type(value = Updated.class, name = Updated.TYPE),
        @JsonSubTypes.Type(value = Deleted.class, name = Deleted.TYPE),
        @JsonSubTypes.Type(value = Created.class, name = Created.TYPE),
})
public interface ChangeMetadata {
    <T> T accept(Visitor<T> visitor);

    static Unchanged unchanged() {
        return ImmutableUnchanged.builder().build();
    }

    static Updated updated(byte[] oldValue, byte[] newValue) {
        return ImmutableUpdated.of(oldValue, newValue);
    }

    static Deleted deleted(byte[] oldValue) {
        return ImmutableDeleted.of(oldValue);
    }

    static Created created(byte[] newValue) {
        return ImmutableCreated.of(newValue);
    }

    interface Visitor<T> {
        T visit(Unchanged unchanged);

        T visit(Updated updated);

        T visit(Deleted deleted);

        T visit(Created created);
    }

    @Immutable
    @JsonSerialize(as = ImmutableUnchanged.class)
    @JsonDeserialize(as = ImmutableUnchanged.class)
    @JsonTypeName(Unchanged.TYPE)
    interface Unchanged extends ChangeMetadata {
        String TYPE = "unchanged";

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    @Immutable
    @JsonSerialize(as = ImmutableUpdated.class)
    @JsonDeserialize(as = ImmutableUpdated.class)
    @JsonTypeName(Updated.TYPE)
    interface Updated extends ChangeMetadata {
        String TYPE = "updated";

        @Value.Parameter
        byte[] oldValue();

        @Value.Parameter
        byte[] newValue();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    @Immutable
    @JsonSerialize(as = ImmutableDeleted.class)
    @JsonDeserialize(as = ImmutableDeleted.class)
    @JsonTypeName(Deleted.TYPE)
    interface Deleted extends ChangeMetadata {
        String TYPE = "deleted";

        @Value.Parameter
        byte[] oldValue();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    @Immutable
    @JsonSerialize(as = ImmutableCreated.class)
    @JsonDeserialize(as = ImmutableCreated.class)
    @JsonTypeName(Created.TYPE)
    interface Created extends ChangeMetadata {
        String TYPE = "created";

        @Value.Parameter
        byte[] newValue();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }
}
