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
package com.palantir.atlasdb.calcite;

import static com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;

public final class ProtobufDeserializers {
    private ProtobufDeserializers() {
        // uninstantiable
    }

    public static Map<String, Object> convertMessageToMap(Message message) {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            FieldDescriptor field = entry.getKey();
            Object value = entry.getValue();
            builder.put(field.getName(), getValue(field, value));
        }
        return builder.build();
    }

    private static Object getValue(FieldDescriptor field, Object value) {
        if (field.isRepeated()) {
            return ((List<?>) value).toArray();
        } else {
            return value;
        }
    }
}
