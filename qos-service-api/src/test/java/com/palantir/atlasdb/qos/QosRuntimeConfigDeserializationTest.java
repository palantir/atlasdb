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

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.collect.ImmutableMap;

public class QosRuntimeConfigDeserializationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory()
            .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

    @Test
    public void canDeserializeQosServerConfiguration() throws IOException {
        File testConfigFile = new File(QosServiceRuntimeConfig.class.getResource("/qos.yml").getPath());
        QosServiceRuntimeConfig configuration = OBJECT_MAPPER.readValue(testConfigFile, QosServiceRuntimeConfig.class);

        assertThat(configuration).isEqualTo(ImmutableQosServiceRuntimeConfig.builder()
                .clientLimits(ImmutableMap.of("test", 10L, "test2", 20L))
                .build());
    }
}
