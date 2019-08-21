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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ws.rs.core.HttpHeaders;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.service.UserAgents;

public class AtlasDbHttpProtocolVersionTest {
    private static final UserAgent USER_AGENT_1 = createBaseUserAgent("Bond", "0.0.7");
    private static final UserAgent USER_AGENT_2 = createBaseUserAgent("Smith", "1.2.3");

    private static final UserAgent.Agent AGENT_1 = UserAgent.Agent.of("Shield", "24.7.0");
    private static final UserAgent.Agent CONJURE_AGENT = UserAgent.Agent.of(UserAgents.CONJURE_AGENT_NAME, "6.6.6");

    private final HttpHeaders httpHeaders = mock(HttpHeaders.class);

    @Test
    public void detectsConjureJavaRuntimeAgent() {
        when(httpHeaders.getRequestHeader(HttpHeaders.USER_AGENT)).thenReturn(
                ImmutableList.of(UserAgents.format(USER_AGENT_1.addAgent(CONJURE_AGENT))));

        assertThat(AtlasDbHttpProtocolVersion.inferFromHttpHeaders(httpHeaders))
                .isEqualTo(AtlasDbHttpProtocolVersion.CONJURE_JAVA_RUNTIME);
    }

    @Test
    public void returnsLegacyIfConjureJavaRuntimeAgentNotPresent() {
        when(httpHeaders.getRequestHeader(HttpHeaders.USER_AGENT)).thenReturn(
                ImmutableList.of(UserAgents.format(USER_AGENT_1.addAgent(AGENT_1))));

        assertThat(AtlasDbHttpProtocolVersion.inferFromHttpHeaders(httpHeaders))
                .isEqualTo(AtlasDbHttpProtocolVersion.LEGACY_OR_UNKNOWN);
    }

    @Test
    public void returnsLegacyIfNoUserAgentHeaderPresent() {
        when(httpHeaders.getRequestHeader(HttpHeaders.USER_AGENT)).thenReturn(null);

        assertThat(AtlasDbHttpProtocolVersion.inferFromHttpHeaders(httpHeaders))
                .isEqualTo(AtlasDbHttpProtocolVersion.LEGACY_OR_UNKNOWN);
    }

    @Test
    public void returnsLegacyIfUserAgentHeaderUnparseable() {
        when(httpHeaders.getRequestHeader(HttpHeaders.USER_AGENT)).thenReturn(
                ImmutableList.of("I don't know what a user agent is"));

        assertThat(AtlasDbHttpProtocolVersion.inferFromHttpHeaders(httpHeaders))
                .isEqualTo(AtlasDbHttpProtocolVersion.LEGACY_OR_UNKNOWN);
    }

    @Test
    public void detectsConjureJavaRuntimeAgentInPresenceOfUnparseableHeaders() {
        when(httpHeaders.getRequestHeader(HttpHeaders.USER_AGENT)).thenReturn(
                ImmutableList.of("Ich weiß nicht, was ein 'User-Agent' bedeutet",
                        UserAgents.format(USER_AGENT_1.addAgent(CONJURE_AGENT))));

        assertThat(AtlasDbHttpProtocolVersion.inferFromHttpHeaders(httpHeaders))
                .isEqualTo(AtlasDbHttpProtocolVersion.CONJURE_JAVA_RUNTIME);
    }

    @Test
    public void detectsConjureJavaRuntimeAgentOnAnyPresentUserAgentHeader() {
        when(httpHeaders.getRequestHeader(HttpHeaders.USER_AGENT)).thenReturn(
                ImmutableList.of(
                        UserAgents.format(USER_AGENT_1),
                        UserAgents.format(USER_AGENT_2.addAgent(CONJURE_AGENT))));

        assertThat(AtlasDbHttpProtocolVersion.inferFromHttpHeaders(httpHeaders))
                .isEqualTo(AtlasDbHttpProtocolVersion.CONJURE_JAVA_RUNTIME);
    }

    private static UserAgent createBaseUserAgent(String conjureAgentName, String version) {
        return UserAgent.of(UserAgent.Agent.of(conjureAgentName, version));
    }
}
