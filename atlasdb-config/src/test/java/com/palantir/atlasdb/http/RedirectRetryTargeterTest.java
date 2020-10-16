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

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class RedirectRetryTargeterTest {
    private static final URL URL_1 = createUrlUnchecked("https", "hostage", 42, "/request/hourai-branch");
    private static final URL URL_2 = createUrlUnchecked("https", "hostile", 424, "/request/swallows-cowrie-shell");
    private static final URL URL_3 = createUrlUnchecked("https", "hostel", 4242, "/request/fire-rat-robe");

    @Test
    public void redirectsToSelfIfOnlyOneNode() {
        RedirectRetryTargeter targeter = RedirectRetryTargeter.create(URL_1, ImmutableList.of(URL_1));
        assertThat(targeter.redirectRequest(Optional.empty())).isEmpty();
    }

    @Test
    public void redirectsToOtherNodesIfLeaderUnknown() {
        RedirectRetryTargeter targeter = RedirectRetryTargeter.create(URL_2, ImmutableList.of(URL_1, URL_2, URL_3));
        Map<URL, List<URL>> results = IntStream.range(0, 10000)
                .boxed()
                .map($ -> targeter.redirectRequest(Optional.empty()).get())
                .collect(Collectors.groupingBy(Function.identity()));
        assertThat(results.keySet()).containsExactlyInAnyOrder(URL_1, URL_3);
    }

    @Test
    public void redirectsToLeaderIfLeaderKnown() {
        RedirectRetryTargeter targeter = RedirectRetryTargeter.create(URL_2, ImmutableList.of(URL_1, URL_2, URL_3));
        Map<URL, List<URL>> results = IntStream.range(0, 10000)
                .boxed()
                .map($ -> targeter.redirectRequest(Optional.of(hostAndPort(URL_1)))
                        .get())
                .collect(Collectors.groupingBy(Function.identity()));
        assertThat(results.keySet()).containsOnly(URL_1);
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

    private static HostAndPort hostAndPort(URL url) {
        return HostAndPort.fromParts(url.getHost(), url.getPort());
    }
}
