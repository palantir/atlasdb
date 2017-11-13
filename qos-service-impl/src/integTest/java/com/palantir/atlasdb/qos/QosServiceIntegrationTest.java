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

import java.util.Optional;

import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.atlasdb.http.AtlasDbHttpClients;

public class QosServiceIntegrationTest {

    private static final String SERVER_URI = "http://localhost:5080";

    @ClassRule
    public static QosServerHolder serverHolder = new QosServerHolder("server.yml");
    private static QosService service = AtlasDbHttpClients.createProxy(
            Optional.empty(),
            SERVER_URI,
            QosService.class,
            "integration tests");

    @Test
    public void returnsConfiguredLimits() {
        assertThat(service.getLimit("test")).isEqualTo(10L);
        assertThat(service.getLimit("test2")).isEqualTo(20L);
    }

}
