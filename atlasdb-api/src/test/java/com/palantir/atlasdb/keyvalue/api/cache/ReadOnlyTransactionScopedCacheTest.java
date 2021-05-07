/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.cache;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class ReadOnlyTransactionScopedCacheTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");
    private static final Cell CELL = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("sanity"));
    private static final byte[] VALUE = PtBytes.toBytes("valuable");

    @Mock
    public TransactionScopedCache delegate;

    @Test
    public void writeAndDeleteThrows() {
        TransactionScopedCache readOnlyCache = ReadOnlyTransactionScopedCache.create(delegate);

        assertThatThrownBy(() -> readOnlyCache.write(TABLE, ImmutableMap.of(CELL, VALUE)))
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Cannot write via the read only transaction cache");

        assertThatThrownBy(() -> readOnlyCache.delete(TABLE, ImmutableSet.of(CELL)))
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Cannot delete via the read only transaction cache");
    }
}
