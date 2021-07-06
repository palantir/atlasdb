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
package com.palantir.atlasdb.schema.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.table.description.ValueType;
import org.junit.Test;

public class StreamStoreDefinitionBuilderTest {

    @Test
    public void testTooBigInMemoryThreshold() {
        assertThatThrownBy(() -> new StreamStoreDefinitionBuilder("test", "test", ValueType.VAR_LONG)
                        .inMemoryThreshold(StreamStoreDefinition.MAX_IN_MEMORY_THRESHOLD + 1)
                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testMaxInMemoryThreshold() {
        assertThat(new StreamStoreDefinitionBuilder("test", "test", ValueType.VAR_LONG)
                        .inMemoryThreshold(StreamStoreDefinition.MAX_IN_MEMORY_THRESHOLD)
                        .build())
                .isNotNull();
    }
}
