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
package com.palantir.nexus.db.sql;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;

public class SQLStringTest {
    @Test
    public void testCanonicalizeString() {
        List<String> testQuery = ImmutableList.of(
                "insert foo into bar ; ",
                "insert\nfoo into bar",
                "insert\n \tfoo into bar",
                "insert foo into bar;",
                "insert  foo into bar;   ",
                "insert  foo into bar; ;;  ",
                "\tinsert foo into bar;",
                "\t insert foo into bar;",
                "insert  foo into bar;\n;;  ",
                "   insert \t\nfoo \n \tinto  \n\rbar\n\t;   ",
                "/* UnregisteredSQLString */ insert foo into bar;",
                "  /* UnregisteredSQLString */insert foo into bar",
                "  /* UnregisteredSQLString */insert foo into bar ");
        String canonicalQuery = "insert foo into bar";

        testQuery.forEach(sql -> assertEquals(canonicalQuery, SQLString.canonicalizeString(sql)));
    }

    @Test
    public void testCanonicalizeBatch() {
        List<String> testBatch = ImmutableList.of(
                "/* UnregisteredSQLString */ insert foo into bar; /* UnregisteredSQLString */insert foo into bar;",
                "insert foo into bar; /* UnregisteredSQLString */ insert foo into bar");
        String canonicalBatch = "insert foo into bar; insert foo into bar";

        testBatch.forEach(sql -> assertEquals(canonicalBatch, SQLString.canonicalizeString(sql)));
    }

    @Test
    public void testCanonicalizeStringAndRemoveWhitespaceEntirely() {
        List<String> testBatch = ImmutableList.of(
                "/* UnregisteredSQLString */ insert foo into bar; /* UnregisteredSQLString */insert foo into bar;",
                "insert foo into bar; /* UnregisteredSQLString */ insert foo into bar");
        String canonicalBatch = "insertfoointobar;insertfoointobar";

        testBatch.forEach(sql -> assertEquals(canonicalBatch, SQLString.canonicalizeStringAndRemoveWhitespaceEntirely(sql)));
    }

    @Test
    public void testCanonicalizeBlanks() throws Exception {
        List<String> testBatch = ImmutableList.of("",
                " ",
                " ;; ; ");
        testBatch.forEach(sql -> assertEquals("", SQLString.canonicalizeString(sql)));

    }
}
