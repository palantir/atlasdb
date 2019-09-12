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
    private static final URL URL_1;
    private static final URL URL_2;
    private static final URL URL_3;

    static {
        try {
            URL_1 = new URL("https", "hostage", 42, "/request/hourai-branch");
            URL_2 = new URL("http", "hostile", 424, "/request/swallows-cowrie-shell");
            URL_3 = new URL("https", "hostel", 4242, "/request/fire-rat-robe");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void redirectsToSelfIfOnlyOneNode() {
        RedirectRetryTargeter targeter = RedirectRetryTargeter.create(URL_1, ImmutableList.of(URL_1));
        assertThat(targeter.redirectRequest()).isEqualTo(URL_1);
    }

    @Test
    public void redirectsToNextNode() {
        RedirectRetryTargeter targeter = RedirectRetryTargeter.create(URL_2, ImmutableList.of(URL_1, URL_2, URL_3));
        assertThat(targeter.redirectRequest()).isEqualTo(URL_3);
    }

    @Test
    public void redirectsAroundLastElement() {
        RedirectRetryTargeter targeter = RedirectRetryTargeter.create(URL_3, ImmutableList.of(URL_1, URL_2, URL_3));
        assertThat(targeter.redirectRequest()).isEqualTo(URL_1);
    }

    @Test
    public void throwsIfNodeUnrecognized() {
        assertThatThrownBy(() -> RedirectRetryTargeter.create(URL_3, ImmutableList.of(URL_1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Local server not found in the list of cluster URLs.");
    }
}
