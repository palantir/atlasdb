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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.logsafe.Preconditions;
import java.util.Set;
import org.immutables.value.Value;

@Value.Immutable
public interface HostIdResult {

    enum Type {
        SUCCESS,
        SOFT_FAILURE,
        HARD_FAILURE
    }

    Type type();

    Set<String> hostIds();

    static HostIdResult success(Iterable<String> hostIds) {
        return builder().type(Type.SUCCESS).hostIds(hostIds).build();
    }

    static HostIdResult hardFailure() {
        return builder().type(Type.HARD_FAILURE).hostIds(Set.of()).build();
    }

    static HostIdResult softFailure() {
        return builder().type(Type.SOFT_FAILURE).hostIds(Set.of()).build();
    }

    @Value.Check
    default void checkHostIdsStateBasedOnResultType() {
        Preconditions.checkArgument(
                !(type().equals(Type.SUCCESS) && hostIds().isEmpty()),
                "It is expected that there should be at least one host id if the result is successful.");
        Preconditions.checkArgument(
                type().equals(Type.SUCCESS) || hostIds().isEmpty(),
                "It is expected that no hostIds should be present when there is a failure.");
    }

    static ImmutableHostIdResult.Builder builder() {
        return ImmutableHostIdResult.builder();
    }
}
