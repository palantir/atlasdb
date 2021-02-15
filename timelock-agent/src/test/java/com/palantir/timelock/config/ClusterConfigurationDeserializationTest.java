/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class ClusterConfigurationDeserializationTest {
    private static final String LOCAL_SERVER = "https://server-2:8421";
    private static final ImmutableSet<String> SERVERS =
            ImmutableSet.of("https://server-1:8421", LOCAL_SERVER, "https://server-3:8421");

    private static final File CLUSTER_CONFIG_NO_TYPE_INFO = getClusterConfigFile("no-type-info");
    private static final File CLUSTER_CONFIG_DEFAULT_TYPE_INFO = getClusterConfigFile("default-type-info");
    private static final File CLUSTER_CONFIG_INVALID_TYPE_INFO = getClusterConfigFile("invalid-type-info");
    private static final File CLUSTER_CONFIG_MALFORMED = getClusterConfigFile("malformed");
    private static final File CLUSTER_CONFIG_KUBERNETES = getClusterConfigFile("kubernetes");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory()
                    .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
            .registerModule(new GuavaModule());

    @BeforeClass
    public static void setUp() {
        OBJECT_MAPPER.registerSubtypes(DefaultClusterConfiguration.class, KubernetesClusterConfiguration.class);
    }

    @Test
    public void canDeserializeClusterConfigurationWithoutTypeInformation() throws IOException {
        assertDefaultClusterConfigurationCorrect(deserializeClusterConfiguration(CLUSTER_CONFIG_NO_TYPE_INFO));
    }

    @Test
    public void canDeserializeClusterConfigurationWithDefaultTypeInformation() throws IOException {
        assertDefaultClusterConfigurationCorrect(deserializeClusterConfiguration(CLUSTER_CONFIG_DEFAULT_TYPE_INFO));
    }

    @Test
    @Ignore // TODO (jkong): Reenable if/when we find a good solution
    public void throwsWhenDeserializingClusterConfigurationWithInvalidTypeInformation() throws IOException {
        assertThatThrownBy(() -> assertDefaultClusterConfigurationCorrect(
                        deserializeClusterConfiguration(CLUSTER_CONFIG_INVALID_TYPE_INFO)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void throwsOnMalformedClusterConfiguration() throws IOException {
        assertThatThrownBy(() -> deserializeClusterConfiguration(CLUSTER_CONFIG_MALFORMED))
                .isInstanceOf(UnrecognizedPropertyException.class)
                .hasMessageContaining("my-node-set");
    }

    @Test
    public void doesNotDeserializeKubernetesClusterConfigurationAsDefaultClusterConfiguration() throws IOException {
        // This is somewhat limited by the design of KubernetesHostnames, but should at least confirm that we're
        // attempting to deserialize a k8s configuration as one, and not as a default configuration
        assertThatThrownBy(() -> deserializeClusterConfiguration(CLUSTER_CONFIG_KUBERNETES))
                .hasMessageContaining("k8s stateful set");
    }

    private void assertDefaultClusterConfigurationCorrect(ClusterConfiguration cluster) {
        assertThat(cluster)
                .isInstanceOf(DefaultClusterConfiguration.class)
                .isNotInstanceOf(KubernetesClusterConfiguration.class);
        assertThat(cluster.clusterMembers()).hasSameElementsAs(SERVERS);
        assertThat(cluster.localServer()).isEqualTo(LOCAL_SERVER);
    }

    private static ClusterConfiguration deserializeClusterConfiguration(File file) throws IOException {
        return OBJECT_MAPPER.readValue(file, ClusterConfiguration.class);
    }

    private static File getClusterConfigFile(String clusterConfigType) {
        return new File(ClusterConfigurationDeserializationTest.class
                .getResource(String.format("/cluster-configuration-%s.yml", clusterConfigType))
                .getPath());
    }
}
