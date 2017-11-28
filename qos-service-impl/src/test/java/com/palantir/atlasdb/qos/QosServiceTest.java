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

package com.palantir.atlasdb.qos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.qos.config.ImmutableQosClientLimitsConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosLimitsConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosServiceRuntimeConfig;
import com.palantir.atlasdb.qos.config.QosClientLimitsConfig;
import com.palantir.atlasdb.qos.config.QosPriority;
import com.palantir.atlasdb.qos.config.QosServiceRuntimeConfig;

public class QosServiceTest {
    private Supplier<QosServiceRuntimeConfig> config = mock(Supplier.class);
    private QosResource resource;

    @Test
    public void defaultsToFixedLimit() {
        when(config.get()).thenReturn(configWithLimits(ImmutableMap.of()));
        resource = new QosResource(config);
        assertThat((int) QosClientLimitsConfig.BYTES_READ_PER_SECOND_PER_CLIENT).isEqualTo(resource.readLimit("foo"));
        assertThat((int) QosClientLimitsConfig.BYTES_WRITTEN_PER_SECOND_PER_CLIENT).isEqualTo(
                resource.writeLimit("foo"));
    }

    @Test
    public void canLiveReloadLimits() {
        when(config.get())
                .thenReturn(configWithLimits(ImmutableMap.of("foo", 10L)))
                .thenReturn(configWithLimits(ImmutableMap.of("foo", 10L)))
                .thenReturn(configWithLimits(ImmutableMap.of("foo", 20L)));
        resource = new QosResource(config);
        assertEquals(10L, resource.readLimit("foo"));
        assertEquals(20L, resource.readLimit("foo"));
    }

    private QosServiceRuntimeConfig configWithLimits(Map<String, Long> limits) {
        Map<String, QosClientLimitsConfig> clientLimits = limits.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> ImmutableQosClientLimitsConfig.builder()
                                .limits(ImmutableQosLimitsConfig.builder()
                                        .writeBytesPerSecond(entry.getValue())
                                        .readBytesPerSecond(entry.getValue())
                                        .build())
                                .clientPriority(QosPriority.HIGH)
                                .build()
                ));
        return ImmutableQosServiceRuntimeConfig.builder()
                .clientLimits(clientLimits)
                .build();
    }
}

