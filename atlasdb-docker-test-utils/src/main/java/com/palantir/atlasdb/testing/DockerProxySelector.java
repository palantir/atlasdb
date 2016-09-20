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
package com.palantir.atlasdb.testing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.palantir.docker.compose.connection.Cluster;

public class DockerProxySelector extends ProxySelector {
    private final InetSocketAddress proxyAddress;
    private final Supplier<ProjectInfoMappings> projectInfo;

    public DockerProxySelector(Cluster containers, Supplier<ProjectInfoMappings> projectInfo) {
        this.proxyAddress = InetSocketAddress.createUnresolved(
                containers.ip(),
                containers.container("proxy").port(1080).getExternalPort());
        this.projectInfo = projectInfo;
    }

    @Override
    public List<Proxy> select(URI uri) {
        ProjectInfoMappings projectInfoMappings = projectInfo.get();
        Set<String> hosts = projectInfoMappings.getHostToIp().keySet();
        Set<String> ips = projectInfoMappings.getIpToHosts().keySet();

        if (hosts.contains(uri.getHost()) || ips.contains(uri.getHost())) {
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
