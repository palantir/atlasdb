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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class QosServiceRuntimeConfigTest {
    @Test
    public void canBuildFromEmptyClientLimits() {
        assertThat(ImmutableQosServiceRuntimeConfig.builder().clientLimits(new HashMap<>()).build())
                .isInstanceOf(QosServiceRuntimeConfig.class);
    }

    @Test
    public void canBuildFromSingleClientLimit() {
        Map<String, Long> clientLimits = new HashMap<>(1);
        clientLimits.put("test_client", 10L);
        assertThat(ImmutableQosServiceRuntimeConfig.builder().clientLimits(clientLimits).build())
                .isInstanceOf(QosServiceRuntimeConfig.class);
    }

    @Test
    public void canBuildFromMultipleClientLimits() {
        Map<String, Long> clientLimits = new HashMap<>(1);
        clientLimits.put("test_client", 10L);
        clientLimits.put("test_client2", 100L);
        assertThat(ImmutableQosServiceRuntimeConfig.builder().clientLimits(clientLimits).build())
                .isInstanceOf(QosServiceRuntimeConfig.class);
    }
}
