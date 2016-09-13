/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Set;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.docker.compose.DockerComposeRule;

public class DockerProxySelector extends ProxySelector {
    private final Set<String> proxiedAddresses;
    private final InetSocketAddress proxyAddress;

    public DockerProxySelector(DockerComposeRule dockerComposeRule) {
        try {
            this.proxiedAddresses = ImmutableSet.copyOf(dockerComposeRule.dockerCompose().ps());
            this.proxyAddress = InetSocketAddress.createUnresolved(
                    dockerComposeRule.containers().ip(),
                    dockerComposeRule.containers().container("proxy").port(1080).getExternalPort());
        } catch (IOException | InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<Proxy> select(URI uri) {
        if (proxiedAddresses.contains(uri.getHost()) || uri.getHost().startsWith("172.")) {
            return ImmutableList.of(new Proxy(Proxy.Type.SOCKS, proxyAddress));
        } else {
            return ImmutableList.of(Proxy.NO_PROXY);
        }
    }

    @Override
    public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
        throw Throwables.propagate(ioe);
    }
}
