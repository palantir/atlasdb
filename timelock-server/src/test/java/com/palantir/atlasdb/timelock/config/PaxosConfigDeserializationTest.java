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

import java.nio.file.Paths;

import com.palantir.remoting2.config.ssl.SslConfiguration;

public class PaxosConfigDeserializationTest extends AbstractTimelockServerConfigDeserializationTest {
    @Override
    public String getConfigFileName() {
        return "/paxosTestConfig.yml";
    }

    public void assertAlgorithmConfigurationCorrect(TimeLockAlgorithmConfiguration configuration) {
        assertThat(configuration).isInstanceOf(PaxosConfiguration.class);

        PaxosConfiguration paxosConfiguration = (PaxosConfiguration) configuration;

        assertSslConfigurationCorrect(paxosConfiguration.sslConfiguration().get());
        assertThat(paxosConfiguration.paxosDataDir()).isEqualTo(Paths.get("var", "data", "paxos").toFile());
    }

    private void assertSslConfigurationCorrect(SslConfiguration sslConfiguration) {
        assertThat(sslConfiguration.trustStorePath()).isEqualTo(Paths.get("var", "security", "trustStore.jks"));
        assertThat(sslConfiguration.keyStorePath().isPresent()).isFalse();
        assertThat(sslConfiguration.keyStorePassword().isPresent()).isFalse();
    }
}
