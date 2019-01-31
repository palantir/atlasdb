/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.common.time;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class NanoTimeTests {

    @Test
    public void testIsBefore_usual() {
        assertThat(new NanoTime(1).isBefore(new NanoTime(2))).isTrue();
    }

    @Test
    public void testIsBefore_not() {
        assertThat(new NanoTime(1).isBefore(new NanoTime(0))).isFalse();
    }

    @Test
    public void testIsBefore_overflow() {
        assertThat(new NanoTime(Long.MAX_VALUE).isBefore(new NanoTime(Long.MIN_VALUE))).isTrue();
    }

    @Test
    public void canBeSerializedAndDeserialized() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        NanoTime nanoTime = NanoTime.now();
        String serialized = mapper.writeValueAsString(nanoTime);
        NanoTime deserialized = mapper.readValue(serialized, NanoTime.class);

        assertEquals(nanoTime, deserialized);
    }
}