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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class SmokeTests {
    @Test
    public void testConnection() {
        AtlasJdbcTestSuite.connect();
    }

    @Test
    public void testFindsTables() throws SQLException {
        Namespace namespace = Namespace.create("testing");
        TableReference table1 = TableReference.create(namespace, "table1");
        TableReference table2 = TableReference.create(namespace, "table2");
        AtlasJdbcTestSuite
                .getAtlasDbServices()
                .getKeyValueService()
                .createTables(
                        ImmutableMap.of(
                                table1, AtlasDbConstants.GENERIC_TABLE_METADATA,
                                table2, AtlasDbConstants.GENERIC_TABLE_METADATA)
                );

        ArrayList<String> allTableNames = Lists.newArrayList();
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            DatabaseMetaData md = conn.getMetaData();
            ResultSet rs = md.getTables(null, null, "%", null);
            while (rs.next()) {
                allTableNames.add(rs.getString(3));
            }
        }
        assertThat(allTableNames, hasItems(table1.getQualifiedName(), table2.getQualifiedName()));
    }
}
