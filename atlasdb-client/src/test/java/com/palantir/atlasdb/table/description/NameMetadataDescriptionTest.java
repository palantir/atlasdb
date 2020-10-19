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

package com.palantir.atlasdb.table.description;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.common.exception.PalantirRuntimeException;
import org.junit.Test;

public class NameMetadataDescriptionTest {
    private static final NameMetadataDescription SIMPLE_NAME_METADATA_DESCRIPTION = NameMetadataDescription.safe(
            "string", ValueType.STRING);

    private static final NameMetadataDescription MULTIPART_NAME_METADATA_DESCRIPTION = NameMetadataDescription.create(
            ImmutableList.of(
                    new NameComponentDescription.Builder()
                            .componentName("alpha")
                            .type(ValueType.VAR_LONG)
                            .byteOrder(TableMetadataPersistence.ValueByteOrder.DESCENDING)
                            .build(),
                    NameComponentDescription.safe("beta", ValueType.SIZED_BLOB),
                    NameComponentDescription.safe("gamma", ValueType.VAR_STRING),
                    NameComponentDescription.safe("omega", ValueType.STRING)));

    private static final byte[] SAMPLE_ALPHA = EncodingUtils.flipAllBits(ValueType.VAR_LONG.convertFromJava(42L));
    private static final byte[] SAMPLE_BETA = ValueType.SIZED_BLOB.convertFromJava(new byte[5]);
    private static final byte[] SAMPLE_GAMMA = ValueType.VAR_STRING.convertFromString("boo");
    private static final byte[] SAMPLE_OMEGA = ValueType.STRING.convertFromString("O(n)");
    private static final byte[] SAMPLE_ROW = EncodingUtils.add(SAMPLE_ALPHA, SAMPLE_BETA, SAMPLE_GAMMA, SAMPLE_OMEGA);
    private static final byte[] SAMPLE_ROW_PREFIX = EncodingUtils.add(SAMPLE_ALPHA, SAMPLE_BETA);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void parseAndRenderAreInverses_Simple() {
        byte[] row = PtBytes.toBytes("theData");
        assertThat(SIMPLE_NAME_METADATA_DESCRIPTION.parseFromJson(
                SIMPLE_NAME_METADATA_DESCRIPTION.renderToJson(row), false)).containsExactly(row);
    }

    @Test
    public void extraFieldsAreTolerated() {
        String extraFieldJson = "{\"string\": \"tom\", \"extraneous\": \"another\"}";
        byte[] result = SIMPLE_NAME_METADATA_DESCRIPTION.parseFromJson(extraFieldJson, false);
        assertThat(result).containsExactly(PtBytes.toBytes("tom"));
    }

    @Test
    // TODO (jkong): Tolerating duplicate fields was permitted, even though it is a bit dubious.
    public void duplicateFieldsAreTolerated() {
        String invalidJson = "{\"string\": \"tom\", \"string\": \"robert\"}";
        byte[] result = SIMPLE_NAME_METADATA_DESCRIPTION.parseFromJson(invalidJson, false);

        // Which value is selected is an implementation detail - we should not guarantee this.
        assertThat(result).satisfiesAnyOf(
                (bytes) -> assertThat(bytes).containsExactly(PtBytes.toBytes("tom")),
                (bytes) -> assertThat(bytes).containsExactly(PtBytes.toBytes("robert")));
    }

    @Test
    public void throwsIfNoRelevantFieldsProvided() {
        String missingFields = "{\"type\": \"string\"}";

        assertThatThrownBy(() -> SIMPLE_NAME_METADATA_DESCRIPTION.parseFromJson(missingFields, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JSON object needs a field named: string.");
    }

    @Test
    public void throwsOnRawJsonString() {
        String jsonString = "\"string\"";

        assertThatThrownBy(() -> SIMPLE_NAME_METADATA_DESCRIPTION.parseFromJson(jsonString, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Only JSON objects can be deserialized into parsed byte arrays.");
    }

    @Test
    public void throwsOnArrays() {
        String jsonString = "[\"string\"]";

        assertThatThrownBy(() -> SIMPLE_NAME_METADATA_DESCRIPTION.parseFromJson(jsonString, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Only JSON objects can be deserialized into parsed byte arrays.");
    }

    @Test
    public void throwsOnNonJsonInput() {
        String gobbledygook = "]q2!a0v-_13r";

        assertThatThrownBy(() -> SIMPLE_NAME_METADATA_DESCRIPTION.parseFromJson(gobbledygook, false))
                .isInstanceOf(PalantirRuntimeException.class)
                .hasMessageContaining("Unexpected close marker");
    }

    @Test
    public void parseAndRenderAreInverses_MultiPart() {
        assertThat(MULTIPART_NAME_METADATA_DESCRIPTION.parseFromJson(
                MULTIPART_NAME_METADATA_DESCRIPTION.renderToJson(SAMPLE_ROW), false)).containsExactly(SAMPLE_ROW);
    }

    @Test
    public void missingFieldsAreNotToleratedWithoutPrefix() throws JsonProcessingException {
        JsonNode jsonNode = OBJECT_MAPPER.readTree(MULTIPART_NAME_METADATA_DESCRIPTION.renderToJson(SAMPLE_ROW));
        ((ObjectNode) jsonNode).remove(ImmutableList.of("gamma", "omega"));

        assertThatThrownBy(() -> MULTIPART_NAME_METADATA_DESCRIPTION.parseFromJson(jsonNode.toString(), false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JSON object has 2 defined fields, but the number of row components is 4.");
    }

    @Test
    public void missingSuffixFieldsAreToleratedInPrefixMode() throws JsonProcessingException {
        JsonNode jsonNode = OBJECT_MAPPER.readTree(MULTIPART_NAME_METADATA_DESCRIPTION.renderToJson(SAMPLE_ROW));
        ((ObjectNode) jsonNode).remove(ImmutableList.of("gamma", "omega"));

        byte[] bytes = MULTIPART_NAME_METADATA_DESCRIPTION.parseFromJson(jsonNode.toString(), true);
        assertThat(bytes).containsExactly(SAMPLE_ROW_PREFIX);
    }

    @Test
    public void missingNonSuffixFieldsAreNotToleratedInPrefixMode() throws JsonProcessingException {
        JsonNode jsonNode = OBJECT_MAPPER.readTree(MULTIPART_NAME_METADATA_DESCRIPTION.renderToJson(SAMPLE_ROW));
        ((ObjectNode) jsonNode).remove(ImmutableList.of("alpha", "omega"));

        assertThatThrownBy(() -> MULTIPART_NAME_METADATA_DESCRIPTION.parseFromJson(jsonNode.toString(), true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JSON object is missing field: alpha");
    }

    @Test
    public void differentOrderingOfFieldsInSerializedFormIsAcceptable() {
        String beginWithEnd = "{\"omega\": \"O(n)\", \"alpha\": 42, \"beta\": \"AAAAAAA==\", \"gamma\": \"boo\"}";
        assertThat(MULTIPART_NAME_METADATA_DESCRIPTION.parseFromJson(beginWithEnd, false)).containsExactly(SAMPLE_ROW);
    }

    @Test
    public void differentOrderingOfFieldsInSerializedPrefixIsAcceptable() {
        String jumbledPrefix = "{\"beta\": \"AAAAAAA==\", \"alpha\": 42}";
        assertThat(MULTIPART_NAME_METADATA_DESCRIPTION.parseFromJson(jumbledPrefix, true))
                .containsExactly(SAMPLE_ROW_PREFIX);
    }
}
