/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.paxos.PaxosResponse;
import java.util.Map;
import java.util.UUID;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableSingleNodeUpdateResponse.class)
@JsonSerialize(as = ImmutableSingleNodeUpdateResponse.class)
@Value.Immutable
public interface SingleNodeUpdateResponse extends PaxosResponse {
    /**
     * other namespaces will not have been disabled/re-enabled (the transaction will not complete).
     */
    Map<Namespace, UUID> lockedNamespaces();

    static SingleNodeUpdateResponse successful() {
        return ImmutableSingleNodeUpdateResponse.builder().isSuccessful(true).build();
    }

    static SingleNodeUpdateResponse of(boolean wasSuccessful, Map<Namespace, UUID> lockedNamespaces) {
        return ImmutableSingleNodeUpdateResponse.builder()
                .isSuccessful(wasSuccessful)
                .lockedNamespaces(lockedNamespaces)
                .build();
    }

    // TODO(gs): check to validate success/locked state?
}
