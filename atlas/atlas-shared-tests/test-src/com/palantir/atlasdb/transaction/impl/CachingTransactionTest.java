// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.transaction.impl;

import java.util.Set;
import java.util.SortedMap;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.transaction.api.Transaction;

@Ignore
public class CachingTransactionTest extends AtlasDbTestCase {
    @Test
    public void testCacheEmptyGets() {
        final String TABLE = "table";
        final Set<byte[]> ONE_ROW = ImmutableSortedSet.<byte[]>orderedBy(PtBytes.BYTES_COMPARATOR).add("row".getBytes()).build();
        final Set<byte[]> NO_ROWS = ImmutableSortedSet.<byte[]>orderedBy(PtBytes.BYTES_COMPARATOR).build();
        final ColumnSelection ALL_COLUMNS = ColumnSelection.all();
        final SortedMap<byte[], RowResult<byte[]>> emptyResults = ImmutableSortedMap.<byte[], RowResult<byte[]>>orderedBy(PtBytes.BYTES_COMPARATOR).build();

        final Mockery m = new Mockery();
        final Transaction t = m.mock(Transaction.class);
        final CachingTransaction c = new CachingTransaction(t);

        m.checking(new Expectations() {{
            oneOf(t).getRows(TABLE, ONE_ROW, ALL_COLUMNS); will(returnValue(emptyResults));
            oneOf(t).getRows(TABLE, NO_ROWS, ALL_COLUMNS); will(returnValue(emptyResults));
        }});

        Assert.assertEquals(emptyResults, c.getRows(TABLE, ONE_ROW, ALL_COLUMNS));
        Assert.assertEquals(emptyResults, c.getRows(TABLE, ONE_ROW, ALL_COLUMNS));

        m.assertIsSatisfied();
    }
}
