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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.MalformedURLException;
import java.net.URL;

import org.junit.Test;

public class RedirectRetryTargeterTest {
    private static final String SCHEME_1 = "http";
    private static final String SCHEME_2 = "https";

    private static final String HOST_1 = "hostage";
    private static final String HOST_2 = "hostile";

    private static final int PORT_1 = 42;
    private static final int PORT_2 = 4222;

    private static final String PATH_1 = "/api/masyu/";
    private static final String PATH_2 = "/api/snake/";
    private static final String REQUEST_PATH_1 = "request/jewelled-branch-of-hourai";
    private static final String REQUEST_PATH_2 = "/request/swallows-cowrie-shell";

    private static final URL BASE_URL_1;
    private static final URL BASE_URL_2;

    static {
        try {
            BASE_URL_1 = new URL(SCHEME_1, HOST_1, PORT_1, PATH_1);
            BASE_URL_2 = new URL(SCHEME_2, HOST_2, PORT_2, PATH_2);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private static final RedirectRetryTargeter SELF_TARGETER = new RedirectRetryTargeter(BASE_URL_1, BASE_URL_1);
    private static final RedirectRetryTargeter REDIRECTING_TARGETER = new RedirectRetryTargeter(BASE_URL_1, BASE_URL_2);

    @Test
    public void getRequestPathAvoidsContext() throws MalformedURLException {
        URL requestUrl = new URL(SCHEME_1, HOST_1, PORT_1, PATH_1 + REQUEST_PATH_1);
        assertThat(REDIRECTING_TARGETER.getRequestPath(requestUrl)).isEqualTo(REQUEST_PATH_1);
    }

    @Test
    public void getRequestPathThrowsIfPathIsNotContextPrefixed() throws MalformedURLException {
        URL requestUrl = new URL(SCHEME_1, HOST_1, PORT_1, REQUEST_PATH_1);
        assertThatThrownBy(() -> REDIRECTING_TARGETER.getRequestPath(requestUrl))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("not in our context path");
    }

    @Test
    public void canRedirectToSelf() throws MalformedURLException {
        URL requestUrl = new URL(SCHEME_1, HOST_1, PORT_1, PATH_1 + REQUEST_PATH_1);
        assertThat(SELF_TARGETER.redirectRequest(requestUrl)).isEqualTo(requestUrl);
    }

    @Test
    public void canRedirectToOtherNodes() throws MalformedURLException {
        URL requestUrl = new URL(SCHEME_1, HOST_1, PORT_1, PATH_1 + REQUEST_PATH_1);
        assertThat(REDIRECTING_TARGETER.redirectRequest(requestUrl)).isEqualTo(
                new URL(SCHEME_2, HOST_2, PORT_2, PATH_2 + REQUEST_PATH_1));
    }

    @Test
    public void handlesRepeatedContextPathCorrectly() throws MalformedURLException {
        URL requestUrl = new URL(SCHEME_1, HOST_1, PORT_1, PATH_1 + PATH_1.substring(1));
        assertThat(REDIRECTING_TARGETER.redirectRequest(requestUrl)).isEqualTo(
                new URL(SCHEME_2, HOST_2, PORT_2, PATH_2 + PATH_1.substring(1)));
    }

    @Test
    public void handlesRequestPathWithLeadingSlash() throws MalformedURLException {
        URL requestUrl = new URL(SCHEME_1, HOST_1, PORT_1, PATH_1 + REQUEST_PATH_2);
        assertThat(REDIRECTING_TARGETER.redirectRequest(requestUrl)).isEqualTo(
                new URL(SCHEME_2, HOST_2, PORT_2, PATH_2 + REQUEST_PATH_2.substring(1)));
    }
}
