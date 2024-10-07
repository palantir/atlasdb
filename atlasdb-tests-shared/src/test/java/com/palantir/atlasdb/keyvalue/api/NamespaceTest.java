/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class NamespaceTest {

    private void expectSuccess(String name, Pattern pattern) {
        assertThat(Namespace.create(name, pattern))
                .isEqualTo(Namespace.createUnchecked(name))
                .satisfies(namespace -> assertThat(namespace.getName()).isEqualTo(name));
        assertThat(Namespace.createUnchecked(name)).isEqualTo(Namespace.create(name, Namespace.UNCHECKED_NAME));
    }

    private void expectFailure(String name, Pattern pattern) {
        assertThatThrownBy(() -> Namespace.create(name, pattern))
                .describedAs("Namespace '" + name + "' was not supposed to match pattern '" + pattern + "'")
                .isInstanceOf(Exception.class);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "namespace",
                "name0space",
                "name_space",
                "nameSpace",
                "n4m3sp4c3",
                "name-space",
                "name__space",
                "|name|space|",
                "!n@a#m$e%s^p&a*c(e)",
                "0-||n4-m3_Sp-4c3-_-",
            })
    public void testValidUncheckedNames(String name) {
        expectSuccess(name, Namespace.UNCHECKED_NAME);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "  ", " \t ", " leading", "trailing ", "int ernal", ".", "name.space"})
    public void testInvalidUncheckedNames(String name) {
        expectFailure(name, Namespace.UNCHECKED_NAME);
        assertThatThrownBy(() -> Namespace.createUnchecked(name))
                .describedAs(
                        "Namespace '" + name + "' should be invalid as it contains whitespace and/or period characters")
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("namespace cannot");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "namespace",
                "name0space",
                "name_space",
                "nameSpace",
                "n4m3sp4c3",
                "name-space",
                "name__space",
            })
    public void testValidLooselyCheckedNames(String name) {
        expectSuccess(name, Namespace.LOOSELY_CHECKED_NAME);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                " ",
                "  ",
                " \t ",
                " leading",
                "trailing ",
                "int ernal",
                ".",
                "name.space",
                "|name|space|",
                "!n@a#m$e%s^p&a*c(e)",
                "0-||n4-m3_Sp-4c3-_-",
            })
    public void testInvalidLooselyCheckedNames(String name) {
        expectFailure(name, Namespace.LOOSELY_CHECKED_NAME);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "namespace",
                "name0space",
                "name_space",
                "nameSpace",
                "n4m3sp4c3",
            })
    public void testValidStrictlyCheckedNames(String name) {
        expectSuccess(name, Namespace.STRICTLY_CHECKED_NAME);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "name-space",
                "name__space",
                "|name|space|",
                "!n@a#m$e%s^p&a*c(e)",
                "0-||n4-m3_Sp-4c3-_-",
                "",
                " ",
                "  ",
                " \t ",
                " leading",
                "trailing ",
                "int ernal",
                ".",
                "name.space",
            })
    public void testInvalidStrictlyCheckedNames(String name) {
        expectFailure(name, Namespace.STRICTLY_CHECKED_NAME);
    }

    @Test
    public void throwsOnNullNames() {
        assertThatThrownBy(() -> Namespace.create(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Namespace.create(null, Namespace.UNCHECKED_NAME))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Namespace.createUnchecked(null)).isInstanceOf(NullPointerException.class);
    }
}
