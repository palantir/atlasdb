///*
// * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
// *
// * Licensed under the BSD-3 License (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://opensource.org/licenses/BSD-3-Clause
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.palantir.atlasdb.qos;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.junit.Assert.assertEquals;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//import java.nio.file.Paths;
//import java.util.Map;
//import java.util.Optional;
//import java.util.function.Supplier;
//
//import org.junit.Before;
//import org.junit.Test;
//
//import com.google.common.collect.ImmutableMap;
//import com.palantir.atlasdb.qos.config.ImmutableQosServiceRuntimeConfig;
//import com.palantir.atlasdb.qos.config.QosServiceRuntimeConfig;
//import com.palantir.remoting.api.config.service.ServiceConfiguration;
//import com.palantir.remoting.api.config.ssl.SslConfiguration;
//
//public class QosServiceTest {
//
//    private Supplier<QosServiceRuntimeConfig> config = mock(Supplier.class);
//    private QosResource resource;
//    private ServiceConfiguration mockCassandraServiceConfig = mock(ServiceConfiguration.class);
//    private SslConfiguration sslConfiguration = SslConfiguration.of(Paths.get("var/security/trustStore.jks"));
//
//    @Before
//    public void before() {
//        QosServiceRuntimeConfig qosServiceRuntimeConfig = mock(QosServiceRuntimeConfig.class);
//        when(qosServiceRuntimeConfig.clientLimits()).thenReturn(ImmutableMap.of());
//        when(qosServiceRuntimeConfig.cassandraServiceConfig()).thenReturn(Optional.empty());
//
//        resource = new QosResource(config);
//    }
//
//    @Test
//    public void defaultsToNoLimit() {
//        when(config.get()).thenReturn(configWithLimits(ImmutableMap.of()));
//        when(config.get()).thenReturn(configWithLimits(ImmutableMap.of()));
//
//        assertThat(resource.getLimit("foo")).isEqualTo(Long.MAX_VALUE);
//    }
//
//    @Test
//    public void canLiveReloadLimits() {
//        when(config.get())
//                .thenReturn(configWithLimits(ImmutableMap.of("foo", 10)))
//                .thenReturn(configWithLimits(ImmutableMap.of("foo", 20)));
//
//        assertEquals(10, resource.getLimit("foo"));
//        assertEquals(20, resource.getLimit("foo"));
//    }
//
//    private QosServiceRuntimeConfig configWithLimits(Map<String, Integer> limits) {
//        return ImmutableQosServiceRuntimeConfig.builder()
//                .clientLimits(limits)
//                .cassandraServiceConfig(mockCassandraServiceConfig)
//                .build();
//    }
//}
//
