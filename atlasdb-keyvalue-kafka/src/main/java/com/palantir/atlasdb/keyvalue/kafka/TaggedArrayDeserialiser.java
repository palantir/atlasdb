/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.kafka;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.table.description.ValueType;

public class TaggedArrayDeserialiser implements Deserializer<TaggedArray> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public TaggedArray deserialize(String topic, byte[] data) {
        int offset = 0;
        Tag decodedTag = Tag.fromBytes((byte[]) ValueType.SIZED_BLOB.convertToJava(data, offset));
        offset += ValueType.SIZED_BLOB.sizeOf(decodedTag.getBytes());

        byte[] decodedData = null;
        if (offset < data.length) {
            decodedData = (byte[]) ValueType.SIZED_BLOB.convertToJava(data, offset);
            offset += ValueType.SIZED_BLOB.sizeOf(decodedData);
        }

        Preconditions.checkArgument(data.length == offset, "Data is too long. %s > %s", data.length, offset);

        return ImmutableTaggedArray.builder()
                .tag(decodedTag)
                .data(decodedData)
                .build();
    }

    @Override
    public void close() {
    }
}
