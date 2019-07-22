/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.migration;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.transaction.impl.TransactionTestSetup;

public class LiveMigratorTest extends TransactionTestSetup {
    private static final TableReference OLD_TABLE_REF = TableReference.createFromFullyQualifiedName("old.table");
    private static final TableReference NEW_TABLE_REF = TableReference.createFromFullyQualifiedName("new.table");

    @ClassRule
    public static final TestResourceManager TRM = TestResourceManager.inMemory();

    public LiveMigratorTest() {
        super(TRM, TRM);
    }

    @Test
    public void test() {
        System.out.println("blablabla");
        KeyValueService kvs = TRM.getDefaultKvs();

        kvs.createTable(OLD_TABLE_REF, AtlasDbConstants.GENERIC_TABLE_METADATA);

        Cell cell = createCell("rowName", "columnName");
        kvs.put(OLD_TABLE_REF,
                ImmutableMap.of(cell, PtBytes.toBytes("value")),
                10L);

        assertThat(kvs.get(OLD_TABLE_REF, ImmutableMap.of(cell, 11L)).get(cell).getContents())
                .containsExactly(PtBytes.toBytes("value"));
    }

    Cell createCell(String rowName, String columnName) {
        return Cell.create(PtBytes.toBytes(rowName), PtBytes.toBytes(columnName));
    }
}