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

package com.palantir.atlasdb.http;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.service.UserAgents;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class ProtocolAwareness {

    public static AtlasDbHttpProtocolVersion getHttpProtocolVersion(@Nullable Collection<String> userAgentHeaders) {
        return AtlasDbHttpProtocolVersion.inferFromString(Optional.ofNullable(userAgentHeaders)
                .flatMap(ProtocolAwareness::parseProtocolVersionFromUserAgentHeader));
    }

    @VisibleForTesting
    static Optional<String> parseProtocolVersionFromUserAgentHeader(@Nonnull Collection<String> userAgentHeaders) {
        return userAgentHeaders.stream()
                .map(UserAgents::tryParse)
                .map(UserAgent::informational)
                .flatMap(List::stream)
                .filter(agent -> agent.name().equals(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT))
                .findFirst()
                .map(UserAgent.Agent::version);
    }

    private ProtocolAwareness() {}
}
