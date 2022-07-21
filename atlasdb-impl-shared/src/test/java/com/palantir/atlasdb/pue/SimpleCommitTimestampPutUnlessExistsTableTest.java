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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TimestampEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
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
public class SimpleCommitTimestampPutUnlessExistsTableTest {
    @Parameterized.Parameter
    public TimestampEncodingStrategy<TransactionStatus> encodingStrategy;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {{V1EncodingStrategy.INSTANCE}, {TicketsEncodingStrategy.INSTANCE}});
    }

    private PutUnlessExistsTable<Long, TransactionStatus> pueTable;

    @Before
    public void setup() {
        pueTable = createPueTable();
    }

    @Test
    public void canPutAndGet() throws ExecutionException, InterruptedException {
        pueTable.putUnlessExists(1L, TransactionStatuses.committed(2L));
        assertThat(TransactionStatuses.getCommitTimestamp(pueTable.get(1L).get()))
                .hasValue(2L);
    }

    @Test
    public void emptyReturnsInProgress() throws ExecutionException, InterruptedException {
        assertThat(TransactionStatuses.caseOf(pueTable.get(3L).get())
                        .inProgress_(true)
                        .otherwise_(false))
                .isTrue();
    }

    @Test
    public void cannotPueTwice() {
        pueTable.putUnlessExists(1L, TransactionStatuses.committed(2L));
        assertThatThrownBy(() -> pueTable.putUnlessExists(1L, TransactionStatuses.committed(2L)))
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
        pueTable.putUnlessExistsMultiple(inputs);
        Map<Long, TransactionStatus> result =
                pueTable.get(ImmutableList.of(1L, 3L, 5L, 7L)).get();
        assertThat(result.size()).isEqualTo(4);
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(1L))).hasValue(2L);
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(3L))).hasValue(4L);
        assertThat(TransactionStatuses.caseOf(result.get(5L)).inProgress_(true).otherwise_(false))
                .isTrue();
        assertThat(TransactionStatuses.getCommitTimestamp(result.get(7L))).hasValue(8L);
    }

    private PutUnlessExistsTable<Long, TransactionStatus> createPueTable() {
        return new SimpleCommitTimestampPutUnlessExistsTable(
                new InMemoryKeyValueService(true),
                TableReference.createFromFullyQualifiedName("test.table"),
                encodingStrategy);
    }
}
