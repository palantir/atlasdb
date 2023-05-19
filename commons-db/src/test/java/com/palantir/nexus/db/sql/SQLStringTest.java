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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.palantir.nexus.db.DBType;
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

        testQuery.forEach(sql -> assertThat(SQLString.canonicalizeString(sql)).isEqualTo(canonicalQuery));
    }

    @Test
    public void testCanonicalizeBatch() {
        List<String> testBatch = ImmutableList.of(
                "/* UnregisteredSQLString */ insert foo into bar; /* UnregisteredSQLString */insert foo into bar;",
                "insert foo into bar; /* UnregisteredSQLString */ insert foo into bar");
        String canonicalBatch = "insert foo into bar; insert foo into bar";

        testBatch.forEach(sql -> assertThat(SQLString.canonicalizeString(sql)).isEqualTo(canonicalBatch));
    }

    @Test
    public void testCanonicalizeStringAndRemoveWhitespaceEntirely() {
        List<String> testBatch = ImmutableList.of(
                "/* UnregisteredSQLString */ insert foo into bar; /* UnregisteredSQLString */insert foo into bar;",
                "insert foo into bar; /* UnregisteredSQLString */ insert foo into bar");
        String canonicalBatch = "insertfoointobar;insertfoointobar";

        testBatch.forEach(sql -> assertThat(SQLString.canonicalizeStringAndRemoveWhitespaceEntirely(sql))
                .isEqualTo(canonicalBatch));
    }

    @Test
    public void testCanonicalizeBlanks() throws Exception {
        List<String> testBatch = ImmutableList.of("", " ", " ;; ; ");
        testBatch.forEach(sql -> assertThat(SQLString.canonicalizeString(sql)).isEmpty());
    }

    @Test
    public void testMakeCommentString() {
        String sql = "SELECT 1 FROM dual";
        assertThat(SQLString.prependPrefix(null, null, sql))
                .isEqualTo("/* UnregisteredSQLString */ SELECT 1 FROM dual");
        assertThat(SQLString.prependPrefix(null, DBType.POSTGRESQL, sql))
                .isEqualTo("/* UnregisteredSQLString dbType: POSTGRESQL */ SELECT 1 FROM dual");
        assertThat(SQLString.prependPrefix(null, DBType.ORACLE, sql))
                .isEqualTo("/* UnregisteredSQLString dbType: ORACLE */ SELECT 1 FROM dual");
        assertThat(SQLString.prependPrefix("test", DBType.POSTGRESQL, sql))
                .isEqualTo("/* SQLString Identifier: test dbType: POSTGRESQL */ SELECT 1 FROM dual");
        assertThat(SQLString.prependPrefix("test", DBType.ORACLE, sql))
                .isEqualTo("/* SQLString Identifier: test dbType: ORACLE */ SELECT 1 FROM dual");
    }

    @Test
    public void createUnregistered() {
        String sql = "SELECT 1 FROM dual";
        assertThat(SQLString.getUnregisteredQuery(sql)).isNotNull().satisfies(unregisteredQuery -> {
            assertThat(unregisteredQuery.getKey()).isNull();
            assertThat(unregisteredQuery.getQuery()).isEqualTo("/* UnregisteredSQLString */ SELECT 1 FROM dual");
        });
        assertThat(new SQLString(null, sql, null)).satisfies(sqlString -> {
            assertThat(sqlString.getKey()).isNull();
            assertThat(sqlString.getQuery()).isEqualTo("/* UnregisteredSQLString */ SELECT 1 FROM dual");
        });
        assertThat(new SQLString(null, sql, DBType.POSTGRESQL)).satisfies(sqlString -> {
            assertThat(sqlString.getKey()).isNull();
            assertThat(sqlString.getQuery())
                    .isEqualTo("/* UnregisteredSQLString dbType: POSTGRESQL */ SELECT 1 FROM dual");
        });
    }

    @Test
    public void createRegistered() {
        String sql = "SELECT 1 from dual";
        assertThat(new SQLString("testKey", sql, null)).satisfies(sqlString -> {
            assertThat(sqlString.getKey()).isEqualTo("testKey");
            assertThat(sqlString.getQuery()).isEqualTo("/* SQLString Identifier: testKey */ SELECT 1 from dual");
        });
        assertThat(new SQLString("testKey", sql, DBType.POSTGRESQL)).satisfies(sqlString -> {
            assertThat(sqlString.getKey()).isEqualTo("testKey");
            assertThat(sqlString.getQuery())
                    .isEqualTo("/* SQLString Identifier: testKey dbType: POSTGRESQL */ SELECT 1 from dual");
        });
    }
}
