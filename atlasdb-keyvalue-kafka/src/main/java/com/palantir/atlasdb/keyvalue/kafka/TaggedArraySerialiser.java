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

import org.apache.kafka.common.serialization.Serializer;

import com.palantir.atlasdb.table.description.ValueType;

public class TaggedArraySerialiser implements Serializer<TaggedArray> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, TaggedArray taggedArray) {
        byte[] encodedTag = ValueType.SIZED_BLOB.convertFromJava(taggedArray.getTag().getBytes());
        if (taggedArray.getData() == null) {
            return encodedTag;
        }

        byte[] encodedData = ValueType.SIZED_BLOB.convertFromJava(taggedArray.getData());

        return CommandTypeSerialisers.combineArrays(encodedTag, encodedData);
    }

    @Override
    public void close() {
    }
}
