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
package com.palantir.atlasdb.table.description;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.util.Pair;
import java.util.UUID;
import org.junit.Test;

public class ValueTypeTest {
    private static final String BYTE_ARRAY = "byte[]";

    @Test
    public void getJavaClassNameReturnsSimpleClassNames() {
        assertThat(ValueType.FIXED_LONG.getJavaClassName()).isEqualTo("long");
    }

    @Test
    public void arrayTypesReturnSimpleClassNames() {
        assertThat(ValueType.BLOB.getJavaClassName()).isEqualTo(BYTE_ARRAY);
    }

    @Test
    public void getJavaObjectClassNameReturnsSimpleObjectClassName() {
        assertThat(ValueType.FIXED_LONG.getJavaObjectClassName()).isEqualTo("Long");
    }

    @Test
    public void arrayTypesReturnClassNameForObjectClassName() {
        assertThat(ValueType.BLOB.getJavaClassName()).isEqualTo(BYTE_ARRAY);
    }

    @Test
    public void stringTypesConvertToAndFromJsonStrings() {
        String string = "tom";
        String jsonString = "\"" + string + "\"";
        assertThat(ValueType.VAR_STRING.convertFromJson(jsonString))
                .isEqualTo(ValueType.VAR_STRING.convertFromJava(string));
        assertThat(ValueType.VAR_STRING.convertToJson(ValueType.VAR_STRING.convertFromJava(string), 0))
                .isEqualTo(Pair.create(jsonString, 4));
        assertThat(ValueType.STRING.convertFromJson(jsonString)).isEqualTo(ValueType.STRING.convertFromJava(string));
        assertThat(ValueType.STRING.convertToJson(ValueType.STRING.convertFromJava(string), 0))
                .isEqualTo(Pair.create(jsonString, 3));
    }

    @Test
    public void illegalArgumentExceptionThrownOnNonJsonStrings() {
        assertThatThrownBy(() -> ValueType.STRING.convertFromJson("{\"data\": 42}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a JSON string");
        assertThatThrownBy(() -> ValueType.STRING.convertFromJson("[2, 3, 4, 5, 6]"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a JSON string");
        assertThatThrownBy(() -> ValueType.STRING.convertFromJson("103-jv1-qg58n"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a JSON string");
    }

    @Test
    public void uuidJsonConversions() {
        UUID uuid = UUID.randomUUID();
        byte[] uuidBytes = ValueType.UUID.convertFromJava(uuid);
        String quotedUuidString = "\"" + uuid.toString() + "\"";
        assertThat(ValueType.UUID.convertToJson(uuidBytes, 0)).isEqualTo(Pair.create(quotedUuidString, 16));
        assertThat(ValueType.UUID.convertFromJson(quotedUuidString)).isEqualTo(uuidBytes);
    }
}
