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

package com.palantir.lock.logger;

import com.fasterxml.jackson.annotation.JsonValue;
import com.palantir.lock.LockClient;
import com.palantir.logsafe.Safe;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * Corresponds to {@link LockClient#getClientId()}, which is safe.
 */
@Safe
@Value.Immutable
public interface ClientId {
    @JsonValue
    @Value.Parameter
    String get();

    /**
     * Null client ID on the API means "anonymous"; replace with empty
     * string.
     */
    static ClientId of(@Nullable String apiClientId) {
        if (apiClientId == null) {
            return ImmutableClientId.of("");
        } else {
            return ImmutableClientId.of(apiClientId);
        }
    }
}
