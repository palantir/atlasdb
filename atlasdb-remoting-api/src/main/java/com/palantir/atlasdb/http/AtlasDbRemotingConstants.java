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

import com.palantir.conjure.java.api.config.service.UserAgent;

public final class AtlasDbRemotingConstants {
    public static final String ATLASDB_HTTP_CLIENT = "atlasdb-http-client";
    public static final AtlasDbHttpProtocolVersion CURRENT_CLIENT_PROTOCOL_VERSION =
            AtlasDbHttpProtocolVersion.CONJURE_JAVA_RUNTIME;
    public static final UserAgent.Agent LEGACY_ATLASDB_HTTP_CLIENT_AGENT = UserAgent.Agent.of(
            ATLASDB_HTTP_CLIENT, AtlasDbHttpProtocolVersion.LEGACY_OR_UNKNOWN.getProtocolVersionString());
    public static final UserAgent.Agent ATLASDB_HTTP_CLIENT_AGENT =
            UserAgent.Agent.of(ATLASDB_HTTP_CLIENT, CURRENT_CLIENT_PROTOCOL_VERSION.getProtocolVersionString());

    public static final UserAgent DEFAULT_USER_AGENT =
            UserAgent.of(UserAgent.Agent.of("unknown", UserAgent.Agent.DEFAULT_VERSION));

    private AtlasDbRemotingConstants() {
        // constants
    }
}
