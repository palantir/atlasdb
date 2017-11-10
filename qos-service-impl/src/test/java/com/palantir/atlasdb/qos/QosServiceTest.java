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

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.qos.config.ImmutableQosServiceRuntimeConfig;
import com.palantir.atlasdb.qos.config.QosServiceRuntimeConfig;

public class QosServiceTest {

    private Supplier<QosServiceRuntimeConfig> config = mock(Supplier.class);
    private QosResource resource = new QosResource(config);

    @Test
    public void defaultsToNoLimit() {
        when(config.get()).thenReturn(configWithLimits(ImmutableMap.of()));

        assertThat(resource.getLimit("foo")).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void canLiveReloadLimits() {
        when(config.get())
                .thenReturn(configWithLimits(ImmutableMap.of("foo", 10L)))
                .thenReturn(configWithLimits(ImmutableMap.of("foo", 20L)));

        assertEquals(10L, resource.getLimit("foo"));
        assertEquals(20L, resource.getLimit("foo"));
    }

    private QosServiceRuntimeConfig configWithLimits(Map<String, Long> limits) {
        return ImmutableQosServiceRuntimeConfig.builder().clientLimits(limits).build();
    }
}

