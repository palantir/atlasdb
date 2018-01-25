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

import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.qos.com.palantir.atlasdb.qos.agent.QosClientConfigLoader;
import com.palantir.atlasdb.qos.config.ImmutableQosClientLimitsConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosLimitsConfig;
import com.palantir.atlasdb.qos.config.QosClientLimitsConfig;
import com.palantir.atlasdb.qos.ratelimit.CassandraMetricsClientLimitMultiplier;
import com.palantir.atlasdb.qos.ratelimit.OneReturningClientLimitMultiplier;

public class QosServiceTest {
    private QosResource resource;

    @Test
    public void defaultsToFixedLimit() {
        QosClientConfigLoader qosClientConfigLoader = QosClientConfigLoader.create(ImmutableMap::of);
        OneReturningClientLimitMultiplier oneReturningClientLimitMultiplier =
                OneReturningClientLimitMultiplier.INSTANCE;
        resource = new QosResource(qosClientConfigLoader, oneReturningClientLimitMultiplier);
        assertThat(QosClientLimitsConfig.BYTES_READ_PER_SECOND_PER_CLIENT).isEqualTo(resource.readLimit("foo"));
        assertThat(QosClientLimitsConfig.BYTES_WRITTEN_PER_SECOND_PER_CLIENT).isEqualTo(resource.writeLimit("foo"));
    }

    @Test
    public void canLiveReloadLimits() {
        QosClientConfigLoader qosClientConfigLoader = QosClientConfigLoader.create(
                new Supplier<Map<String, QosClientLimitsConfig>>() {
                    Iterator<Map<String, QosClientLimitsConfig>> configIterator =
                            ImmutableList.of(
                                    configWithLimits(ImmutableMap.of("foo", 10L)),
                                    configWithLimits(ImmutableMap.of("foo", 20L)),
                                    configWithLimits(ImmutableMap.of("foo", 30L)))
                                    .iterator();

                    @Override
                    public Map<String, QosClientLimitsConfig> get() {
                        return configIterator.next();
                    }
                });
        resource = new QosResource(qosClientConfigLoader, OneReturningClientLimitMultiplier.create());
        assertEquals(10L, resource.readLimit("foo"));
        assertEquals(20L, resource.readLimit("foo"));
        assertEquals(30L, resource.readLimit("foo"));
    }

    @Test
    public void canScaleDownLimits() {
        QosClientConfigLoader qosClientConfigLoader = QosClientConfigLoader.create(() -> ImmutableMap.of("foo",
                ImmutableQosClientLimitsConfig.builder()
                        .limits(ImmutableQosLimitsConfig.builder()
                                .readBytesPerSecond(100)
                                .writeBytesPerSecond(20)
                                .build())
                        .build()));
        CassandraMetricsClientLimitMultiplier clientLimitMultiplier = mock(CassandraMetricsClientLimitMultiplier.class);
        resource = new QosResource(qosClientConfigLoader, clientLimitMultiplier);
        when(clientLimitMultiplier.getClientLimitMultiplier()).thenReturn(1.0, 1.0, 0.5, 0.5, 0.25,
                0.25);
        assertEquals(100L, resource.readLimit("foo"));
        assertEquals(20L, resource.writeLimit("foo"));
        assertEquals(50L, resource.readLimit("foo"));
        assertEquals(10L, resource.writeLimit("foo"));
        assertEquals(25L, resource.readLimit("foo"));
        assertEquals(5L, resource.writeLimit("foo"));
    }

    @Test
    public void canScaleDownAndScaleUpLimits() {
        QosClientConfigLoader qosClientConfigLoader = QosClientConfigLoader.create(() -> ImmutableMap.of("foo",
                ImmutableQosClientLimitsConfig.builder()
                        .limits(ImmutableQosLimitsConfig.builder()
                                .readBytesPerSecond(100)
                                .writeBytesPerSecond(20)
                                .build())
                        .build()));
        CassandraMetricsClientLimitMultiplier clientLimitMultiplier = mock(CassandraMetricsClientLimitMultiplier.class);
        resource = new QosResource(qosClientConfigLoader, clientLimitMultiplier);
        when(clientLimitMultiplier.getClientLimitMultiplier()).thenReturn(1.0, 1.0, 0.5, 0.5, 0.55,
                0.55);
        assertEquals(100L, resource.readLimit("foo"));
        assertEquals(20L, resource.writeLimit("foo"));
        assertEquals(50L, resource.readLimit("foo"));
        assertEquals(10L, resource.writeLimit("foo"));
        assertEquals(55L, resource.readLimit("foo"));
        assertEquals(11L, resource.writeLimit("foo"));
    }

    private Map<String, QosClientLimitsConfig> configWithLimits(Map<String, Long> limits) {
        return limits.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> ImmutableQosClientLimitsConfig.builder()
                                .limits(ImmutableQosLimitsConfig.builder()
                                        .writeBytesPerSecond(entry.getValue())
                                        .readBytesPerSecond(entry.getValue())
                                        .build())
                                .build()));
    }
}

