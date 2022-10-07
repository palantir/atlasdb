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

package com.palantir.atlasdb.atomic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TransactionStatusEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatuses;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SimpleCommitTimestampAtomicTableTest {
    @Parameterized.Parameter
    public TransactionStatusEncodingStrategy<TransactionStatus> encodingStrategy;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {{V1EncodingStrategy.INSTANCE}, {TicketsEncodingStrategy.INSTANCE}});
    }

    private AtomicTable<Long, TransactionStatus> atomicTable;

    @Before
    public void setup() {
        atomicTable = createPueTable();
    }

    @Test
    public void canPutAndGet() throws ExecutionException, InterruptedException {
        atomicTable.update(1L, TransactionStatuses.committed(2L));
        assertThat(TransactionStatuses.getCommitTimestamp(atomicTable.get(1L).get()))
                .hasValue(2L);
    }

    @Test
    public void emptyReturnsInProgress() throws ExecutionException, InterruptedException {
        assertThat(atomicTable.get(3L).get()).isEqualTo(TransactionConstants.IN_PROGRESS);
    }

    @Test
    public void cannotPueTwice() {
        atomicTable.update(1L, TransactionStatuses.committed(2L));
        assertThatThrownBy(() -> atomicTable.update(1L, TransactionStatuses.committed(2L)))
                .isInstanceOf(KeyAlreadyExistsException.class);
    }

    @Test
    public void canPutAndGetMultiple() throws ExecutionException, InterruptedException {
        ImmutableMap<Long, TransactionStatus> inputs = ImmutableMap.of(
                1L,
                TransactionStatuses.committed(2L),
                3L,
                TransactionStatuses.committed(4L),
                7L,
                TransactionStatuses.committed(8L));
        atomicTable.updateMultiple(inputs);
        Map<Long, TransactionStatus> result =
                atomicTable.get(ImmutableList.of(1L, 3L, 5L, 7L)).get();
        assertThat(result).hasSize(4);
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(1L))).hasValue(2L);
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(3L))).hasValue(4L);
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(7L))).hasValue(8L);
    }

    @Test
    public void canPutAndGetAbortedTransactions() throws ExecutionException, InterruptedException {
        ImmutableMap<Long, TransactionStatus> inputs = ImmutableMap.of(1L, TransactionStatuses.aborted());
        atomicTable.updateMultiple(inputs);
        Map<Long, TransactionStatus> result =
                atomicTable.get(ImmutableList.of(1L)).get();
        assertThat(result).hasSize(1);
        assertThat(result.get(1L)).isEqualTo(TransactionConstants.ABORTED);
    }

    @Test
    public void getsAllTimestamps() throws ExecutionException, InterruptedException {
        ImmutableMap<Long, TransactionStatus> inputs = ImmutableMap.of(1L, TransactionStatuses.committed(2L));
        atomicTable.updateMultiple(inputs);
        Map<Long, TransactionStatus> result =
                atomicTable.get(ImmutableList.of(1L, 3L)).get();
        assertThat(result).hasSize(2);
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(1L))).hasValue(2L);
        assertThat(result.get(3L)).isEqualTo(TransactionConstants.IN_PROGRESS);
    }

    @Test
    public void doesNotThrowForDuplicateValues() throws ExecutionException, InterruptedException {
        ImmutableMap<Long, TransactionStatus> inputs = ImmutableMap.of(1L, TransactionStatuses.committed(2L));
        atomicTable.updateMultiple(inputs);

        // does not throw any exception
        Map<Long, TransactionStatus> result =
                atomicTable.get(ImmutableList.of(1L, 1L, 3L)).get();

        assertThat(result).hasSize(2);
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(1L))).hasValue(2L);
        assertThat(result.get(3L)).isEqualTo(TransactionConstants.IN_PROGRESS);
    }

    private AtomicTable<Long, TransactionStatus> createPueTable() {
        return new SimpleCommitTimestampAtomicTable(
                new InMemoryKeyValueService(true),
                TableReference.createFromFullyQualifiedName("test.table"),
                encodingStrategy);
    }
}
