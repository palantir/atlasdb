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
package com.palantir.atlasdb.keyvalue.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Objects;
import org.junit.Test;

public class TableReferenceTest {
    @Test
    public void createLowerCasedWithNamespace() {
        String upperFoo = "FOO";
        String upperBar = "BAR";

        TableReference upper = TableReference.create(Namespace.create(upperFoo), upperBar);
        TableReference lower = TableReference.createLowerCased(upper);

        assertThat(upperFoo.toLowerCase()).isEqualTo(lower.getNamespace().getName());
        assertThat(upperBar.toLowerCase()).isEqualTo(lower.getTableName());
    }

    @Test
    public void createLowerCasedWithoutNamespace() {
        String upperBar = "BAR";

        TableReference upper = TableReference.createWithEmptyNamespace(upperBar);
        TableReference lower = TableReference.createLowerCased(upper);

        assertThat(Namespace.EMPTY_NAMESPACE).isEqualTo(lower.getNamespace());
        assertThat(upperBar.toLowerCase()).isEqualTo(lower.getTableName());
    }

    @Test
    public void hashCodeShouldBeCompatibleWithObjectsHash() {
        assertThat(TableReference.create(Namespace.create("test"), "table"))
                .describedAs("Should have hashCode compatible with Objects.hash(Object...)")
                .hasSameHashCodeAs(new Object() {
                    @Override
                    public int hashCode() {
                        return Objects.hash(Namespace.create("test"), "table");
                    }
                })
                .doesNotHaveSameHashCodeAs(TableReference.create(Namespace.create("table"), "test"));
    }

    @Test
    public void testSizeInBytes() {
        assertThat(TableReference.createWithEmptyNamespace("").sizeInBytes()).isEqualTo(0);
        assertThat(TableReference.createWithEmptyNamespace("FOO").sizeInBytes()).isEqualTo(3 * Character.BYTES);
        assertThat(TableReference.createWithEmptyNamespace("FOOBA").sizeInBytes())
                .isEqualTo(5 * Character.BYTES);
        assertThat(TableReference.create(Namespace.create("FOO"), "").sizeInBytes())
                .isEqualTo(3 * Character.BYTES);
        assertThat(TableReference.create(Namespace.create("FOO"), "BAR").sizeInBytes())
                .isEqualTo(6 * Character.BYTES);
        assertThat(TableReference.create(Namespace.create("FOO"), "BABAZ").sizeInBytes())
                .isEqualTo(8 * Character.BYTES);
        assertThat(TableReference.create(Namespace.create("FOOBAR"), "BAZ").sizeInBytes())
                .isEqualTo(9 * Character.BYTES);
    }
}
