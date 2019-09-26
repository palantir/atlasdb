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

package com.palantir.atlasdb.config;

import java.util.function.Supplier;

import org.immutables.value.Value;

import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.conjure.java.api.config.service.UserAgent;

/**
 * Additional parameters for clients to specify when connecting to remote services.
 */
@Value.Immutable
public interface AuxiliaryRemotingParameters {
    AuxiliaryRemotingParameters DEFAULT_NO_PAYLOAD_LIMIT = AuxiliaryRemotingParameters.builder()
            .shouldLimitPayload(false)
            .build();
    AuxiliaryRemotingParameters DEFAULT_WITH_PAYLOAD_LIMIT = AuxiliaryRemotingParameters.builder()
            .shouldLimitPayload(true)
            .build();

    @Value.Default
    default UserAgent userAgent() {
        return AtlasDbRemotingConstants.DEFAULT_USER_AGENT;
    }

    boolean shouldLimitPayload();

    @Value.Default
    default Supplier<RemotingClientConfig> remotingClientConfig() {
        return () -> RemotingClientConfig.DEFAULT;
    }

    static ImmutableAuxiliaryRemotingParameters.Builder builder() {
        return ImmutableAuxiliaryRemotingParameters.builder();
    }
}
