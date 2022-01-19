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
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SimpleCommitTimestampPutUnlessExistsTableTest {
    @Parameterized.Parameter
    public TimestampEncodingStrategy<Long> encodingStrategy;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {{V1EncodingStrategy.INSTANCE}, {TicketsEncodingStrategy.INSTANCE}});
    }

    private PutUnlessExistsTable<Long, Long> pueTable;

    @Before
    public void setup() {
        pueTable = createPueTable();
    }

    @Test
    public void canPutAndGet() throws ExecutionException, InterruptedException {
        pueTable.putUnlessExists(1L, 2L);
        assertThat(pueTable.get(1L).get()).isEqualTo(2L);
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

    private PutUnlessExistsTable<Long, Long> createPueTable() {
        return new SimpleCommitTimestampPutUnlessExistsTable(
                new InMemoryKeyValueService(true),
                TableReference.createFromFullyQualifiedName("test.table"),
                encodingStrategy);
    }
}
