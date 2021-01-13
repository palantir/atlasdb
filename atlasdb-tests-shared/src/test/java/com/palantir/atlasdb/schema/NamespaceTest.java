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
package com.palantir.atlasdb.schema;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import java.util.regex.Pattern;
import org.junit.Test;

public class NamespaceTest {

    private void expectSuccess(String string, Pattern pattern) {
        Namespace.create(string, pattern);
    }

    private void expectFailure(String string, Pattern pattern) {
        assertThatThrownBy(() -> Namespace.create(string, pattern))
                .describedAs("Namespace '" + string + "' was not supposed to match pattern '" + pattern + "'")
                .isInstanceOf(Exception.class);
    }

    @Test
    public void testUncheckedNames() {
        expectSuccess("namespace", Namespace.UNCHECKED_NAME);
        expectSuccess("name0space", Namespace.UNCHECKED_NAME);
        expectSuccess("name_space", Namespace.UNCHECKED_NAME);
        expectSuccess("nameSpace", Namespace.UNCHECKED_NAME);
        expectSuccess("n4m3sp4c3", Namespace.UNCHECKED_NAME);
        expectSuccess("name-space", Namespace.UNCHECKED_NAME);
        expectSuccess("name__space", Namespace.UNCHECKED_NAME);
        expectSuccess("|name|space|", Namespace.UNCHECKED_NAME);
        expectSuccess("!n@a#m$e%s^p&a*c(e)", Namespace.UNCHECKED_NAME);
        expectSuccess("0-||n4-m3_Sp-4c3-_-", Namespace.UNCHECKED_NAME);
        expectFailure("", Namespace.UNCHECKED_NAME);
        expectFailure(" leading", Namespace.UNCHECKED_NAME);
        expectFailure("trailing ", Namespace.UNCHECKED_NAME);
        expectFailure("int ernal", Namespace.UNCHECKED_NAME);
        expectFailure(".", Namespace.UNCHECKED_NAME);
        expectFailure("name.space", Namespace.UNCHECKED_NAME);
    }

    @Test
    public void testLooselyCheckedNames() {
        expectSuccess("namespace", Namespace.LOOSELY_CHECKED_NAME);
        expectSuccess("name0space", Namespace.LOOSELY_CHECKED_NAME);
        expectSuccess("name_space", Namespace.LOOSELY_CHECKED_NAME);
        expectSuccess("nameSpace", Namespace.LOOSELY_CHECKED_NAME);
        expectSuccess("n4m3sp4c3", Namespace.LOOSELY_CHECKED_NAME);
        expectSuccess("name-space", Namespace.LOOSELY_CHECKED_NAME);
        expectSuccess("name__space", Namespace.LOOSELY_CHECKED_NAME);
        expectFailure("|name|space|", Namespace.LOOSELY_CHECKED_NAME);
        expectFailure("!n@a#m$e%s^p&a*c(e)", Namespace.LOOSELY_CHECKED_NAME);
        expectFailure("0-||n4-m3_Sp-4c3-_-", Namespace.LOOSELY_CHECKED_NAME);
        expectFailure("", Namespace.LOOSELY_CHECKED_NAME);
        expectFailure(" leading", Namespace.LOOSELY_CHECKED_NAME);
        expectFailure("trailing ", Namespace.LOOSELY_CHECKED_NAME);
        expectFailure("int ernal", Namespace.LOOSELY_CHECKED_NAME);
        expectFailure(".", Namespace.LOOSELY_CHECKED_NAME);
        expectFailure("name.space", Namespace.LOOSELY_CHECKED_NAME);
    }

    @Test
    public void testStrictlyCheckedNames() {
        expectSuccess("namespace", Namespace.STRICTLY_CHECKED_NAME);
        expectSuccess("name0space", Namespace.STRICTLY_CHECKED_NAME);
        expectSuccess("name_space", Namespace.STRICTLY_CHECKED_NAME);
        expectSuccess("nameSpace", Namespace.STRICTLY_CHECKED_NAME);
        expectSuccess("n4m3sp4c3", Namespace.STRICTLY_CHECKED_NAME);
        expectFailure("name-space", Namespace.STRICTLY_CHECKED_NAME);
        expectFailure("name__space", Namespace.STRICTLY_CHECKED_NAME);
        expectFailure("|name|space|", Namespace.STRICTLY_CHECKED_NAME);
        expectFailure("!n@a#m$e%s^p&a*c(e)", Namespace.STRICTLY_CHECKED_NAME);
        expectFailure("0-||n4-m3_Sp-4c3-_-", Namespace.STRICTLY_CHECKED_NAME);
        expectFailure("", Namespace.STRICTLY_CHECKED_NAME);
        expectFailure(" leading", Namespace.STRICTLY_CHECKED_NAME);
        expectFailure("trailing ", Namespace.STRICTLY_CHECKED_NAME);
        expectFailure("int ernal", Namespace.STRICTLY_CHECKED_NAME);
        expectFailure(".", Namespace.STRICTLY_CHECKED_NAME);
        expectFailure("name.space", Namespace.STRICTLY_CHECKED_NAME);
    }
}
