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

import com.google.common.collect.ImmutableList;

public class RedirectRetryTargeterTest {
    private static final URL URL_1 = createUrlUnchecked("https", "hostage", 42, "/request/hourai-branch");
    private static final URL URL_2 = createUrlUnchecked("https", "hostile", 424, "/request/swallows-cowrie-shell");
    private static final URL URL_3 = createUrlUnchecked("https", "hostel", 4242, "/request/fire-rat-robe");

    @Test
    public void redirectsToSelfIfOnlyOneNode() {
        RedirectRetryTargeter targeter = RedirectRetryTargeter.create(URL_1, ImmutableList.of(URL_1));
        assertThat(targeter.redirectRequest()).isEqualTo(URL_1);
    }

    @Test
    public void redirectsToRandomNode() {
        RedirectRetryTargeter targeter = RedirectRetryTargeter.create(URL_2, ImmutableList.of(URL_1, URL_2, URL_3));
        assertThat(targeter.redirectRequest())
                .isIn(URL_1, URL_3);
    }

    @Test
    public void throwsIfNodeUnrecognized() {
        assertThatThrownBy(() -> RedirectRetryTargeter.create(URL_3, ImmutableList.of(URL_1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Local server not found in the list of cluster URLs.");
    }

    private static URL createUrlUnchecked(String protocol, String host, int port, String path) {
        try {
            return new URL(protocol, host, port, path);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
