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

package com.palantir.atlasdb.jackson;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.ValueType;

public class AtlasDeserializersTest {

    @Test
    public void testDeserializeRowFromJsonList() throws Exception {
        JsonNode jsonNode = new ObjectMapper().readTree("[68, \"Smeagol\"]");
        NameMetadataDescription nameMetadataDescription = NameMetadataDescription.create(ImmutableList.of(
                new NameComponentDescription("age", ValueType.FIXED_LONG),
                new NameComponentDescription("name", ValueType.STRING)));
        byte[] row = AtlasDeserializers.deserializeRow(nameMetadataDescription, jsonNode);
        byte[] expectedRow = Bytes.concat(ValueType.FIXED_LONG.convertFromString("68"),
                                          ValueType.STRING.convertFromString("Smeagol"));
        Assert.assertArrayEquals(expectedRow, row);
    }

    @Test
    public void testDeserializeRowFromJsonMap() throws Exception {
        JsonNode jsonNode = new ObjectMapper().readTree("{\"age\":68, \"name\":\"Smeagol\"}");
        NameMetadataDescription nameMetadataDescription = NameMetadataDescription.create(ImmutableList.of(
                new NameComponentDescription("age", ValueType.FIXED_LONG),
                new NameComponentDescription("name", ValueType.STRING)));
        byte[] row = AtlasDeserializers.deserializeRow(nameMetadataDescription, jsonNode);
        byte[] expectedRow = Bytes.concat(ValueType.FIXED_LONG.convertFromString("68"),
                                          ValueType.STRING.convertFromString("Smeagol"));
        Assert.assertArrayEquals(expectedRow, row);
    }
}
