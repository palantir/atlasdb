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

package com.palantir.atlasdb.table.generation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.transaction.api.Transaction;
import java.util.List;
import org.junit.Test;

public class TriggersTest {

    public static final String[] EMPTY_STRING_ARRAY = new String[0];

    @Test
    public void emptyTriggersDoesNotAllocate() {
        Transaction mockTxn = mock(Transaction.class);
        List<Function<? super Transaction, String>> sharedTriggers = ImmutableList.of();

        assertThat(Triggers.getAllTriggers(mockTxn, sharedTriggers, EMPTY_STRING_ARRAY))
                .hasSize(0)
                .isSameAs(ImmutableList.of())
                .isEmpty();

        verifyNoInteractions(mockTxn);
    }

    @Test
    public void oneSharedTrigger() {
        Transaction mockTxn = mock(Transaction.class);
        List<Function<? super Transaction, String>> sharedTriggers = ImmutableList.of(t -> "1");

        assertThat(Triggers.getAllTriggers(mockTxn, sharedTriggers, EMPTY_STRING_ARRAY))
                .hasSize(1)
                .containsExactly("1");

        verifyNoInteractions(mockTxn);
    }

    @Test
    public void oneLocalTrigger() {
        Transaction mockTxn = mock(Transaction.class);
        List<Function<? super Transaction, String>> sharedTriggers = ImmutableList.of();

        assertThat(Triggers.getAllTriggers(mockTxn, sharedTriggers, new String[] {"l0"}))
                .hasSize(1)
                .containsExactly("l0");

        verifyNoInteractions(mockTxn);
    }

    @Test
    public void sharedAndLocalTriggers() {
        Transaction mockTxn = mock(Transaction.class);
        List<Function<? super Transaction, String>> sharedTriggers = ImmutableList.of(t -> "s1", t -> "s2");

        assertThat(Triggers.getAllTriggers(mockTxn, sharedTriggers, new String[] {"l0"}))
                .hasSize(3)
                .containsExactly("l0", "s1", "s2");

        verifyNoInteractions(mockTxn);
    }
}
