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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public final class NanoTimeTests {

    @Test
    public void testIsBefore_usual() {
        assertThat(NanoTime.createForTests(1).isBefore(NanoTime.createForTests(2)))
                .isTrue();
    }

    @Test
    public void testIsBefore_not() {
        assertThat(NanoTime.createForTests(1).isBefore(NanoTime.createForTests(0)))
                .isFalse();
    }

    @Test
    public void testIsBefore_overflow() {
        assertThat(NanoTime.createForTests(Long.MAX_VALUE).isBefore(NanoTime.createForTests(Long.MIN_VALUE)))
                .isTrue();
    }

    @Test
    public void canBeSerializedAndDeserialized() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        NanoTime nanoTime = NanoTime.now();
        String serialized = mapper.writeValueAsString(nanoTime);
        NanoTime deserialized = mapper.readValue(serialized, NanoTime.class);

        assertThat(deserialized).isEqualTo(nanoTime);
    }
}
