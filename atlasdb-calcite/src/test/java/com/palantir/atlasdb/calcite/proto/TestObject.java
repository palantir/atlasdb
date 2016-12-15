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
package com.palantir.atlasdb.calcite.proto;

import java.util.Map;

import com.google.common.base.Preconditions;

public final class TestObject {
    private TestObject() {
        // uninstantiable
    }

    public static TestProtobufs.TestObject from(Object map) {
        Preconditions.checkArgument(map instanceof Map, "Given object must be of type Map not type " + map.getClass());
        return from((Map) map);
    }

    public static TestProtobufs.TestObject from(Map<String, Object> map) {
        return TestProtobufs.TestObject.newBuilder()
                .setId((Long) map.get("id"))
                .setType((Long) map.get("type"))
                .setValid((Boolean) map.get("valid"))
                .build();
    }

    public static TestProtobufs.TestObject seededBy(int i) {
        return TestProtobufs.TestObject.newBuilder()
                .setId(10L + i)
                .setType(543L + 10 * i)
                .setValid(i % 2 == 0)
                .build();
    }
}
