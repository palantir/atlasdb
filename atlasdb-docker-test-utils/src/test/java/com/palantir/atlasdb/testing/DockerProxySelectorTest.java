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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Supplier;

import org.junit.Test;

import com.palantir.docker.compose.connection.Cluster;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.ContainerCache;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.ImmutableCluster;

public class DockerProxySelectorTest {
    private static final String CLUSTER_IP = "172.17.0.1";
    private static final int PROXY_EXTERNAL_PORT = 12345;
    private static final InetSocketAddress PROXY_ADDRESS = InetSocketAddress
            .createUnresolved(CLUSTER_IP, PROXY_EXTERNAL_PORT);

    private final Supplier<ProjectInfoMappings> mappings = mock(Supplier.class);
    private final DockerProxySelector dockerProxySelector = new DockerProxySelector(setupProxyContainer(), mappings);

    @Test
    public void nonDockerAddressesShouldNotGoThroughAProxy() throws URISyntaxException {
        when(mappings.get()).thenReturn(ImmutableProjectInfoMappings.builder()
                .build());

        assertThat(dockerProxySelector.select(new URI("http://some-address")))
                .containsExactly(Proxy.NO_PROXY);
    }

    @Test
    public void dockerAddressesShouldGoThroughAProxy() throws URISyntaxException {
        when(mappings.get()).thenReturn(ImmutableProjectInfoMappings.builder()
                .putHostToIp("some-address", mock(InetAddress.class))
                .build());

        assertThat(dockerProxySelector.select(new URI("http://some-address")))
                .containsExactly(new Proxy(Proxy.Type.SOCKS, PROXY_ADDRESS));
    }

    @Test
    public void dockerIpsShouldGoThroughAProxy() throws URISyntaxException {
        when(mappings.get()).thenReturn(ImmutableProjectInfoMappings.builder()
                .putIpToHosts("172.17.0.5", "some-address")
                .build());

        assertThat(dockerProxySelector.select(new URI("http://172.17.0.5")))
                .containsExactly(new Proxy(Proxy.Type.SOCKS, PROXY_ADDRESS));
    }

    @Test(expected = RuntimeException.class)
    public void connectionFailedShouldPropagateExceptions() throws URISyntaxException {
        dockerProxySelector.connectFailed(
                new URI("http://some-address"),
                InetSocketAddress.createUnresolved(CLUSTER_IP, PROXY_EXTERNAL_PORT),
                new IOException());
    }

    private static Cluster setupProxyContainer() {
        Container proxyContainer = mock(Container.class);
        when(proxyContainer.port(1080))
                .thenReturn(new DockerPort(CLUSTER_IP, PROXY_EXTERNAL_PORT, 1080));

        ContainerCache containerCache = mock(ContainerCache.class);
        when(containerCache.container("proxy"))
                .thenReturn(proxyContainer);

        return ImmutableCluster.builder()
                .ip(CLUSTER_IP)
                .containerCache(containerCache)
                .build();
    }
}
