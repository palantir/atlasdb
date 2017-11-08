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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class QosServiceResourceTest {
    @Test
    public void canGetUnspecifiedLimit() {
        QosService resource = new QosServiceResource(ImmutableQosServiceRuntimeConfig.builder().build());
        assertEquals(Long.MAX_VALUE, resource.getLimit("test-client"));
    }

    @Test
    public void canGetSpecifiedLimit() {
        QosService resource = new QosServiceResource(ImmutableQosServiceRuntimeConfig.builder()
                .clientLimits(ImmutableMap.of("test", 10L)).build());
        assertEquals(10L, resource.getLimit("test"));
    }

}

