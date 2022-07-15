/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import org.junit.Test;

public class TableNameTest {
    @Test
    public void internalTableName() {
        checkInternalTableNameRoundTrip(Namespace.DEFAULT_NAMESPACE, "_hidden", "default___hidden");
        checkInternalTableNameRoundTrip(Namespace.DEFAULT_NAMESPACE, "foo.bar", "default__foo.bar");
        checkInternalTableNameRoundTrip(Namespace.DEFAULT_NAMESPACE, "test", "default__test");
        checkInternalTableNameRoundTrip(Namespace.EMPTY_NAMESPACE, "_hidden", "_hidden");
        checkInternalTableNameRoundTrip(Namespace.EMPTY_NAMESPACE, "test", "test");
        checkInternalTableNameRoundTrip(Namespace.create("ns"), "_hidden", "ns___hidden");
        checkInternalTableNameRoundTrip(Namespace.create("ns"), "foo.bar", "ns__foo.bar");
        checkInternalTableNameRoundTrip(Namespace.create("ns"), "test", "ns__test");
    }

    private static void checkInternalTableNameRoundTrip(
            Namespace namespace, String tableName, String internalTableName) {
        TableReference tableRef = TableReference.create(namespace, tableName);
        assertThat(AbstractKeyValueService.internalTableName(tableRef)).isEqualTo(internalTableName);
        TableReference fromInternalTableName = TableReference.fromInternalTableName(internalTableName);
        assertThat(fromInternalTableName.getNamespace()).isEqualTo(namespace);
        assertThat(fromInternalTableName.getTableName()).isEqualTo(tableName);
    }
}
