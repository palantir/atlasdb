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

package com.palantir.atlasdb.http;

import java.util.List;

import javax.ws.rs.core.HttpHeaders;

import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.service.UserAgents;

public enum AtlasDbHttpProtocolVersion {
    LEGACY_OR_UNKNOWN,
    CONJURE_JAVA_RUNTIME;

    public static AtlasDbHttpProtocolVersion inferFromHttpHeaders(HttpHeaders headers) {
        List<String> userAgentHeader = headers.getRequestHeader(HttpHeaders.USER_AGENT);
        if (userAgentHeader == null) {
            return LEGACY_OR_UNKNOWN;
        }
        return userAgentHeader
                .stream()
                .map(UserAgents::tryParse)
                .map(UserAgent::informational)
                .flatMap(List::stream)
                .anyMatch(agent -> agent.name().equals(UserAgents.CONJURE_AGENT_NAME))
                ? CONJURE_JAVA_RUNTIME
                : LEGACY_OR_UNKNOWN;
    }
}
