/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.tokens.auth.BearerToken;

@JsonSerialize(as = ImmutableTimeLockRuntimeConfig.class)
@JsonDeserialize(as = ImmutableTimeLockRuntimeConfig.class)
@Value.Immutable
public abstract class TimeLockRuntimeConfig {

    private static final int MIN_TOKEN_LENGTH = 16;

    @Value.Default
    public ServerListConfig serversList() {
        return ImmutableServerListConfig.builder().build();
    }

    /**
     * Token added to the Authorization header of the requests to Timelock server
     * to authorize the request for the specified namespace.
     */
    @JsonProperty("auth-token")
    public abstract BearerToken authToken();

    @Value.Check
    protected void check() {
        Preconditions.checkState(authToken() != null || authToken().getToken().length() >= MIN_TOKEN_LENGTH,
                "The auth token must have at least %s characters.", MIN_TOKEN_LENGTH);
    }
}
