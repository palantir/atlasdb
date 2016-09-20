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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ProxySelector;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.rules.ExternalResource;

import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ProjectName;

import net.amygdalum.xrayinterface.XRayInterface;

public class DockerProxyRule extends ExternalResource {
    private final DockerComposeRule dockerComposeRule;
    private final ProjectName projectName;

    private ProxySelector originalProxySelector;

    public DockerProxyRule(ProjectName projectName) {
        this.dockerComposeRule = DockerComposeRule.builder()
                .file(getDockerComposeFile(projectName).getPath())
                .build();
        this.projectName = projectName;
    }

    @Override
    protected void before() throws Throwable {
        originalProxySelector = ProxySelector.getDefault();
        dockerComposeRule.before();
        ProjectInfo projectInfo = new ProjectInfo(dockerComposeRule.dockerExecutable(), projectName);
        getNameServices().add(0, new DockerNameService(projectInfo));
        ProxySelector.setDefault(new DockerProxySelector(dockerComposeRule.containers(), projectInfo));
    }

    @Override
    protected void after() {
        ProxySelector.setDefault(originalProxySelector);
        getNameServices().remove(0);
        dockerComposeRule.after();
    }

    private static File getDockerComposeFile(ProjectName projectName) {
        try {
            File proxyFile = File.createTempFile("proxy", ".yml");
            String proxyConfig = Resources.toString(
                    Resources.getResource("docker-compose.proxy.yml"),
                    StandardCharsets.UTF_8);
            Files.write(
                    proxyConfig.replace("{{PROJECT_NAME}}", projectName.constructComposeFileCommand().get(1)),
                    proxyFile,
                    StandardCharsets.UTF_8);
            return proxyFile;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
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
