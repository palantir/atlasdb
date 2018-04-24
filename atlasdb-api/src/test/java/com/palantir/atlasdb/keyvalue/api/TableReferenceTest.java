/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.api;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TableReferenceTest {
    @Test
    public void createLowerCasedWithNamespace() {
        String upperFoo = "FOO";
        String upperBar = "BAR";

        TableReference upper = TableReference.create(Namespace.create(upperFoo), upperBar);
        TableReference lower = TableReference.createLowerCased(upper);

        assertEquals(lower.getNamespace().getName(), upperFoo.toLowerCase());
        assertEquals(lower.getTablename(), upperBar.toLowerCase());
    }

    @Test
    public void createLowerCasedWithoutNamespace() {
        String upperBar = "BAR";

        TableReference upper = TableReference.createWithEmptyNamespace(upperBar);
        TableReference lower = TableReference.createLowerCased(upper);

        assertEquals(lower.getNamespace(), Namespace.EMPTY_NAMESPACE);
        assertEquals(lower.getTablename(), upperBar.toLowerCase());
    }

}
