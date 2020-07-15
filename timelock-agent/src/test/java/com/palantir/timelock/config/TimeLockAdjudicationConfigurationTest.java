/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

public class TimeLockAdjudicationConfigurationTest {
    private static final String CONFIG = "enabled: false";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory()
            .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
            .registerModule(new GuavaModule());

    @BeforeClass
    public static void setUp() {
        OBJECT_MAPPER.registerSubtypes(
                DefaultClusterConfiguration.class,
                KubernetesClusterConfiguration.class);
    }

    @Test
    public void canDeserialize() throws JsonProcessingException {
        assertThat(OBJECT_MAPPER.readValue(CONFIG, TimeLockAdjudicationConfiguration.class))
                .satisfies(configuration -> assertThat(configuration.enabled()).isFalse());
    }
}
