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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.logsafe.Preconditions;
import org.immutables.value.Value;

@Value.Immutable
public interface IdSupportingHostIdResult {
    @Value.Parameter
    HostIdResult result();

    static IdSupportingHostIdResult wrap(HostIdResult result) {
        return ImmutableIdSupportingHostIdResult.of(result);
    }

    @Value.Check
    default void check() {
        Preconditions.checkArgument(
                result().type() != HostIdResult.Type.SOFT_FAILURE,
                "Soft failures are not ID supporting and thus not allowed.");
    }
}
