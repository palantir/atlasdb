/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.jackson;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlasdb.persist.api.Persister;
import com.palantir.atlasdb.persister.JacksonPersister;
import java.util.Map;
import org.junit.Test;

public class TableMetadataDeserializerTest {
    private final TableMetadataDeserializer deserializer = new TableMetadataDeserializer();
    private final ObjectMapper MAPPER = new ObjectMapper();

    private final JsonNode legacyNode =
            MAPPER.convertValue(Map.of("format", "PERSISTER", "type", LegacyPersister.class.getName()), JsonNode.class);

    private final JsonNode newNode = MAPPER.convertValue(
            Map.of("format", "PERSISTER", "type", JacksonPersister.class.getName()), JsonNode.class);

    @Test
    public void testLegacyDeserialize() {
        assertThat(deserializer.deserializeValue(legacyNode).getImportClass()).isEqualTo(LegacyPersister.class);
    }

    @Test
    public void testReusableDeserialize() {
        assertThat(deserializer.deserializeValue(newNode).getImportClass()).isEqualTo(JacksonPersister.class);
    }

    public static final class LegacyPersister implements Persister<String> {
        @Override
        public byte[] persistToBytes(String objectToPersist) {
            return null;
        }

        @Override
        public Class<String> getPersistingClassType() {
            return String.class;
        }

        @Override
        public String hydrateFromBytes(byte[] input) {
            return null;
        }
    }
}
