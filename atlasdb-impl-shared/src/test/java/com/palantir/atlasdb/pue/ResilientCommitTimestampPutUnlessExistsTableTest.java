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

package com.palantir.atlasdb.pue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.encoding.TwoPhaseEncodingStrategy;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

public class ResilientCommitTimestampPutUnlessExistsTableTest {
    private final UnreliableInMemoryKvs kvs = new UnreliableInMemoryKvs();
    private final ConsensusForgettingStore spiedStore =
            spy(new KvsConsensusForgettingStore(kvs, TableReference.createFromFullyQualifiedName("test.table")));

    private final PutUnlessExistsTable<Long, Long> pueTable =
            new ResilientCommitTimestampPutUnlessExistsTable(spiedStore, TwoPhaseEncodingStrategy.INSTANCE);

    @Test
    public void canPutAndGet() throws ExecutionException, InterruptedException {
        pueTable.putUnlessExists(1L, 2L);
        assertThat(pueTable.get(1L).get()).isEqualTo(2L);

        verify(spiedStore).putUnlessExists(anyMap());
        verify(spiedStore, atLeastOnce()).put(anyMap());
        verify(spiedStore).getMultiple(any());
    }

    @Test
    public void emptyReturnsNull() throws ExecutionException, InterruptedException {
        assertThat(pueTable.get(3L).get()).isNull();
    }

    @Test
    public void cannotPueTwice() {
        pueTable.putUnlessExists(1L, 2L);
        assertThatThrownBy(() -> pueTable.putUnlessExists(1L, 2L)).isInstanceOf(KeyAlreadyExistsException.class);
    }

    @Test
    public void canPutAndGetMultiple() throws ExecutionException, InterruptedException {
        ImmutableMap<Long, Long> inputs = ImmutableMap.of(1L, 2L, 3L, 4L, 7L, 8L);
        pueTable.putUnlessExistsMultiple(inputs);
        assertThat(pueTable.get(ImmutableList.of(1L, 3L, 5L, 7L)).get()).isEqualTo(inputs);
    }

    @Test
    public void pueThatThrowsIsCorrectedOnGet() throws ExecutionException, InterruptedException {
        kvs.setThrowOnNextPue();
        assertThatThrownBy(() -> pueTable.putUnlessExists(1L, 2L)).isInstanceOf(RuntimeException.class);
        verify(spiedStore, never()).put(anyMap());

        assertThat(pueTable.get(1L).get()).isEqualTo(2L);
        verify(spiedStore).put(anyMap());
    }

    private static class UnreliableInMemoryKvs extends InMemoryKeyValueService {
        private boolean throwOnNextPue = false;

        public UnreliableInMemoryKvs() {
            super(true);
        }

        void setThrowOnNextPue() {
            throwOnNextPue = true;
        }

        @Override
        public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) {
            super.putUnlessExists(tableRef, values);
            if (throwOnNextPue) {
                throwOnNextPue = false;
                throw new RuntimeException("Ohno!");
            }
        }
    }
}
