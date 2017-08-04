/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.deepkin;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.remoting2.tracing.AlwaysSampler;
import com.palantir.remoting2.tracing.Span;
import com.palantir.remoting2.tracing.Tracer;

public class DeepkinTransactionTest {
    public List<Span> capturedSpans;
    public DeepkinTransaction deepkinTransaction;
    public DeepkinReplayingTransaction deepkinReplayingTransaction;
    private Transaction mockedTransaction;
    private TimeOrderedReplayer replayerService;

    @Before
    public void setup() {
        this.capturedSpans = Lists.newArrayList();
        Tracer.setContentTracing(true);
        Tracer.setSampler(AlwaysSampler.INSTANCE);
        Tracer.subscribe("SPANS", capturedSpans::add);
        Tracer.initTrace(Optional.of(true), "test-trace-id");

        this.mockedTransaction = mock(Transaction.class);
        this.replayerService = new TimeOrderedReplayer();
        this.deepkinTransaction = new DeepkinTransaction(mockedTransaction);
        this.deepkinReplayingTransaction = new DeepkinReplayingTransaction(replayerService, null);
    }

    @Test
    public void testGet() {
        TableReference ref = TableReference.create(Namespace.create("test"), "table");
        Set<Cell> cells = Sets.newHashSet(
                Cell.create("row_1".getBytes(), "col_1".getBytes()),
                Cell.create("row_2".getBytes(), "col_2".getBytes())
        );

        Map<Cell, byte[]> ret = cells.stream().collect(Collectors.toMap(Function.identity(), cell ->
                ("value_" + new String(cell.getColumnName()).split("_")[1]).getBytes()));
        when(mockedTransaction.get(ref, cells)).thenReturn(ret);

        Map<Cell, byte[]> firstResult = deepkinTransaction.get(ref, cells);
        assertThat(firstResult.size(), is(2));
        ret.forEach((k, v) -> assertThat(firstResult.get(k), equalTo(v)));

        assertThat(capturedSpans.size(), is(1));
        replayerService.addTrace(capturedSpans);

        Map<Cell, byte[]> result = deepkinReplayingTransaction.get(ref, cells);
        assertThat(result.size(), is(2));
        ret.forEach((k, v) -> assertThat(result.get(k), equalTo(v)));
    }
}
