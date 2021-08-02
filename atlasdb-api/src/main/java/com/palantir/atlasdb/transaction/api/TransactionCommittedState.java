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

package com.palantir.atlasdb.transaction.api;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.DependentState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.FullyCommittedState;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState.RolledBackState;
import org.immutables.value.Value;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = FullyCommittedState.class, name = "f"),
    @JsonSubTypes.Type(value = RolledBackState.class, name = "r"),
    @JsonSubTypes.Type(value = DependentState.class, name = "d")
})
public interface TransactionCommittedState {
    <T> T accept(Visitor<T> visitor);

    @Value.Immutable
    @JsonSerialize(as = ImmutableFullyCommittedState.class)
    @JsonDeserialize(as = ImmutableFullyCommittedState.class)
    @JsonTypeName("f")
    interface FullyCommittedState extends TransactionCommittedState {
        long commitTimestamp();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visitFullyCommitted(this);
        }
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableRolledBackState.class)
    @JsonDeserialize(as = ImmutableRolledBackState.class)
    @JsonTypeName("r")
    interface RolledBackState extends TransactionCommittedState {
        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visitRolledBack(this);
        }
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableDependentState.class)
    @JsonDeserialize(as = ImmutableDependentState.class)
    @JsonTypeName("d")
    interface DependentState extends TransactionCommittedState {
        long commitTimestamp();

        PrimaryTransactionLocator primaryLocator();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visitDependent(this);
        }
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutablePrimaryTransactionLocator.class)
    @JsonDeserialize(as = ImmutablePrimaryTransactionLocator.class)
    interface PrimaryTransactionLocator {
        String namespace();

        long startTimestamp();
    }

    interface Visitor<T> {
        T visitFullyCommitted(FullyCommittedState fullyCommittedState);

        T visitRolledBack(RolledBackState rolledBackState);

        T visitDependent(DependentState dependentState);
    }
}
