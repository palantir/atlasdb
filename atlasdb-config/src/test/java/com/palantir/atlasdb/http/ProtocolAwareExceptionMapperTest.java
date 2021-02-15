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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.service.UserAgents;
import org.junit.Test;

public class ProtocolAwareExceptionMapperTest {
    private static final UserAgent USER_AGENT_1 = createBaseUserAgent("Bond", "0.0.7");
    private static final UserAgent USER_AGENT_2 = createBaseUserAgent("Smith", "1.2.3");

    private static final UserAgent.Agent AGENT_1 = UserAgent.Agent.of("Shield", "24.7.0");
    private static final String HTTP_PROTOCOL_VERSION = "8.0";
    private static final UserAgent.Agent HTTP_CLIENT_AGENT =
            UserAgent.Agent.of(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT, HTTP_PROTOCOL_VERSION);
    private static final String FORMATTED_USER_AGENT_WITH_HTTP_CLIENT =
            UserAgents.format(USER_AGENT_1.addAgent(HTTP_CLIENT_AGENT));

    @Test
    public void detectsAtlasDbHttpClientAgent() {
        assertThat(ProtocolAwareExceptionMapper.parseProtocolVersionFromUserAgentHeader(
                        ImmutableList.of(FORMATTED_USER_AGENT_WITH_HTTP_CLIENT)))
                .contains(HTTP_PROTOCOL_VERSION);
    }

    @Test
    public void returnsEmptyIfNoRelevantAgentsFound() {
        assertThat(ProtocolAwareExceptionMapper.parseProtocolVersionFromUserAgentHeader(
                        ImmutableList.of(UserAgents.format(USER_AGENT_1.addAgent(AGENT_1)))))
                .isEmpty();
    }

    @Test
    public void returnsEmptyIfNoUserAgentsArePresent() {
        assertThat(ProtocolAwareExceptionMapper.parseProtocolVersionFromUserAgentHeader(ImmutableList.of()))
                .isEmpty();
    }

    @Test
    public void returnsEmptyIfUserAgentStringCannotBeParsed() {
        assertThat(ProtocolAwareExceptionMapper.parseProtocolVersionFromUserAgentHeader(
                        ImmutableList.of("I don't know what a user agent is!")))
                .isEmpty();
    }

    @Test
    public void detectsAtlasDbHttpClientAgentInPresenceOfUnparseableHeaders() {
        assertThat(ProtocolAwareExceptionMapper.parseProtocolVersionFromUserAgentHeader(ImmutableList.of(
                        "Ich weiß nicht, was ein 'User-Agent' bedeutet", FORMATTED_USER_AGENT_WITH_HTTP_CLIENT)))
                .contains(HTTP_PROTOCOL_VERSION);
    }

    @Test
    public void detectsAtlasDbHttpClientAgentOnAnyPresentUserAgentHeader() {
        assertThat(ProtocolAwareExceptionMapper.parseProtocolVersionFromUserAgentHeader(ImmutableList.of(
                        UserAgents.format(USER_AGENT_2.addAgent(AGENT_1)), FORMATTED_USER_AGENT_WITH_HTTP_CLIENT)))
                .contains(HTTP_PROTOCOL_VERSION);
    }

    private static UserAgent createBaseUserAgent(String name, String version) {
        return UserAgent.of(UserAgent.Agent.of(name, version));
    }
}
