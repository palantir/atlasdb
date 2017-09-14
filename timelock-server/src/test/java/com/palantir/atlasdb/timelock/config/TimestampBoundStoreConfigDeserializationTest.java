/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk7.Jdk7Module;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.remoting2.config.ssl.SslConfiguration;

import io.dropwizard.jackson.DiscoverableSubtypeResolver;

public class TimestampBoundStoreConfigDeserializationTest extends AbstractTimelockServerConfigDeserializationTest {
    @Override
    public String getConfigFileName() {
        return "/timestampBoundTestConfig.yml";
    }

    @Override
    public void assertAlgorithmConfigurationCorrect(TimeLockAlgorithmConfiguration configuration) {
        assertThat(configuration).isInstanceOf(TimestampBoundStoreConfiguration.class);

        TimestampBoundStoreConfiguration timestampBoundStoreConfiguration = (TimestampBoundStoreConfiguration) configuration;

        assertThat(timestampBoundStoreConfiguration.kvsConfig()).isPresent();
        assertThat(timestampBoundStoreConfiguration.kvsConfig().get()).isEqualTo(new InMemoryAtlasDbConfig());
    }
}
