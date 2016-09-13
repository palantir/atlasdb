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

import java.net.InetAddress;
import java.net.ProxySelector;
import java.util.List;

import org.junit.rules.ExternalResource;

import com.palantir.docker.compose.DockerComposeRule;

import net.amygdalum.xrayinterface.XRayInterface;

public class DockerProxyRule extends ExternalResource {
    private final DockerComposeRule dockerComposeRule;

    private ProxySelector originalProxySelector;

    public DockerProxyRule(DockerComposeRule dockerComposeRule) {
        this.dockerComposeRule = dockerComposeRule;
    }

    @Override
    protected void before() throws Throwable {
        getNameServices().add(0, new DockerNameService(dockerComposeRule));
        originalProxySelector = ProxySelector.getDefault();
        ProxySelector.setDefault(new DockerProxySelector(dockerComposeRule));
    }

    @Override
    protected void after() {
        getNameServices().remove(0);
        ProxySelector.setDefault(originalProxySelector);
    }

    private static List<sun.net.spi.nameservice.NameService> getNameServices() {
        return XRayInterface
                .xray(InetAddress.class)
                .to(OpenInetAddress.class)
                .getNameServices();
    }

    private interface OpenInetAddress {
        List<sun.net.spi.nameservice.NameService> getNameServices();
    }
}
