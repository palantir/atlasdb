/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.qos.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.function.Supplier;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.qos.config.ImmutableQosClientLimitsConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosLimitsConfig;
import com.palantir.atlasdb.qos.config.QosClientLimitsConfig;

public class QosClientConfigLoaderTest {

    private static final String TEST_CLIENT = "foo";
    private static final String UNSPECIFIED_CLIENT = "unspecified";
    private static final ImmutableQosClientLimitsConfig TEST_LIMITS = configWithLimits(10);

    @Test
    public void configLoaderReturnsTheConfig() throws Exception {
        QosClientConfigLoader clientConfigLoader = new QosClientConfigLoader(
                () -> ImmutableMap.of(TEST_CLIENT, TEST_LIMITS));
        assertThat(clientConfigLoader.getConfigForClient(TEST_CLIENT)).isEqualTo(TEST_LIMITS);
        assertThat(clientConfigLoader.getConfigForClient(UNSPECIFIED_CLIENT)).isEqualTo(
                ImmutableQosClientLimitsConfig.builder().build());
    }

    @Test
    public void configLoaderIsLiveReloadable() throws Exception {
        Supplier<Map<String, QosClientLimitsConfig>> configSupplier = mock(Supplier.class);
        QosClientConfigLoader clientConfigLoader = new QosClientConfigLoader(configSupplier);
        when(configSupplier.get())
                .thenReturn(ImmutableMap.of(TEST_CLIENT, configWithLimits(5)))
                .thenReturn(ImmutableMap.of(TEST_CLIENT, configWithLimits(20)))
                .thenReturn(ImmutableMap.of(TEST_CLIENT, configWithLimits(10)));
        assertThat(clientConfigLoader.getConfigForClient(TEST_CLIENT)).isEqualTo(configWithLimits(5));
        assertThat(clientConfigLoader.getConfigForClient(TEST_CLIENT)).isEqualTo(configWithLimits(20));
        assertThat(clientConfigLoader.getConfigForClient(TEST_CLIENT)).isEqualTo(configWithLimits(10));
    }

    private static ImmutableQosClientLimitsConfig configWithLimits(int limit) {
        return ImmutableQosClientLimitsConfig.builder()
                .limits(ImmutableQosLimitsConfig.builder()
                        .readBytesPerSecond(limit)
                        .writeBytesPerSecond(limit)
                        .build())
                .build();
    }
}
