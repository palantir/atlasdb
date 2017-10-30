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

package com.palantir.atlasdb.table.description;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.Test;

import com.palantir.util.crypto.Sha256Hash;

public class ValueTypeTest {

    private static final String UUID_CLASS_NAME = UUID.class.getName();
    private static final String SHA256HASH_CLASS_NAME = Sha256Hash.class.getName();
    private static final String BYTE_ARRAY = "byte[]";

    @Test
    public void javaObjectTypesReturnFullyQualifiedClassNames() {
        assertThat(ValueType.UUID.getJavaClassName()).isEqualTo(UUID_CLASS_NAME);
        assertThat(ValueType.SHA256HASH.getJavaClassName()).isEqualTo(SHA256HASH_CLASS_NAME);
    }

    @Test
    public void nonJavaObjectTypesReturnSimpleClassNames() {
        assertThat(ValueType.FIXED_LONG.getJavaClassName()).isEqualTo("long");
    }

    @Test
    public void arrayTypesReturnSimpleClassNames() {
        assertThat(ValueType.BLOB.getJavaClassName()).isEqualTo(BYTE_ARRAY);
    }

    @Test
    public void javaObjectTypesReturnFullyQualifiedObjectClassNames() {
        assertThat(ValueType.UUID.getJavaObjectClassName()).isEqualTo(UUID_CLASS_NAME);
        assertThat(ValueType.SHA256HASH.getJavaObjectClassName()).isEqualTo(SHA256HASH_CLASS_NAME);
    }

    @Test
    public void nonJavaObjectTypesReturnSimpleObjectClassNames() {
        assertThat(ValueType.FIXED_LONG.getJavaObjectClassName()).isEqualTo("Long");
    }

    @Test
    public void arrayTypesReturnClassNameForObjectClassName() {
        assertThat(ValueType.BLOB.getJavaClassName()).isEqualTo(BYTE_ARRAY);
    }
}
