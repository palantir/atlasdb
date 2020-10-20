/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class MetricsOutput {

    private final JsonNode output;

    public MetricsOutput(JsonNode output) {
        this.output = output;
    }

    public JsonNode getTimer(String name) {
        JsonNode timers = output.get("timers");
        return timers.get(name);
    }

    public void assertContainsTimer(String name) {
        JsonNode timers = output.get("timers");
        assertThat(timers.get(name)).isNotNull();
    }

    public JsonNode getMeter(String name) {
        JsonNode meters = output.get("meters");
        return meters.get(name);
    }

    public void assertContainsHistogram(String name) {
        assertThat(getHistogram(name)).isNotNull();
    }

    public JsonNode getHistogram(String name) {
        JsonNode histograms = output.get("histograms");
        return histograms.get(name);
    }

    public void assertContainsMeter(String name) {
        assertThat(getMeter(name)).isNotNull();
    }

    public void printToStdOut() {
        try {
            System.out.println(new ObjectMapper()
                    .enable(SerializationFeature.INDENT_OUTPUT)
                    .writeValueAsString(output));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
