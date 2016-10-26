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
package com.palantir.atlasdb.containers;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.junit.After;
import org.junit.Test;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.logging.LogCollector;

import net.amygdalum.xrayinterface.XRayInterface;

@SuppressWarnings("checkstyle:IllegalThrows")
public class ContainersTest {
    @After
    public void shutdownContainers() {
        Containers.onShutdown();
        resetDockerProxyRuleLogCollector();
        clearDockerProxyRuleContainerCache();
    }

    @Test
    public void containerStartsUpCorrectly() throws Throwable {
        Containers containers = new Containers(ContainersTest.class)
                .with(new FirstNginxContainer());

        containers.before();
    }

    @Test
    public void multipleContainersStartUpCorrectly() throws Throwable {
        Containers containers = new Containers(ContainersTest.class)
                .with(new FirstNginxContainer());
        containers.before();

        containers = containers.with(new SecondNginxContainer());
        containers.before();
    }

    // There's a bug in docker-compose-rule where executor is not set to null when stopCollecting() is called
    private void resetDockerProxyRuleLogCollector() {
        DockerComposeRule proxyRuleDockerComposeRule = XRayInterface.xray(Containers.DOCKER_PROXY_RULE)
                .to(OpenDockerProxyRule.class)
                .getDockerComposeRule();

        LogCollector proxyRuleLogCollector = XRayInterface.xray(proxyRuleDockerComposeRule)
                .to(OpenDockerComposeRule.class)
                .getLogCollector();

        XRayInterface.xray(proxyRuleLogCollector)
                .to(OpenFileLogCollector.class)
                .setExecutor(null);
    }

    // There's a bug in docker-compose-rule where the containers are cached (so external ports don't get updated)
    private void clearDockerProxyRuleContainerCache() {
        DockerComposeRule proxyRuleDockerComposeRule = XRayInterface.xray(Containers.DOCKER_PROXY_RULE)
                .to(OpenDockerProxyRule.class)
                .getDockerComposeRule();

        XRayInterface.xray(proxyRuleDockerComposeRule.containers().containerCache())
                .to(OpenContainerCache.class)
                .getContainers()
                .clear();
    }

    interface OpenDockerProxyRule {
        DockerComposeRule getDockerComposeRule();
    }

    interface OpenDockerComposeRule {
        LogCollector getLogCollector();
    }

    interface OpenFileLogCollector {
        void setExecutor(ExecutorService executor);
    }

    interface OpenContainerCache {
        Map<String, Container> getContainers();
    }
}
