/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongPredicate;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.transaction.service.DynamicSplittingTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionService;

public class DynamicSplittingTransactionServiceTest {
    private static final long TIMESTAMP_1 = 71;
    private static final long TIMESTAMP_2 = 82;
    private static final long TIMESTAMP_3 = 113;
    private static final long TIMESTAMP_4 = 144;

    private final TransactionService service1 = mock(TransactionService.class);
    private final TransactionService service2 = mock(TransactionService.class);
    private TransactionService splittingService;

    @Before
    public void setUp() {
        splittingService = forPredicate(ts -> ts < 100);
    }

    @After
    public void checkNoMoreInteractions() {
        verifyNoMoreInteractions(service1, service2);
    }

    @Test
    public void getDelegatesToCorrectService() {
        splittingService.get(TIMESTAMP_1);
        splittingService.get(TIMESTAMP_3);

        verify(service1).get(TIMESTAMP_1);
        verify(service2).get(TIMESTAMP_3);
    }

    @Test
    public void putUnlessExistsDelegatesToCorrectService() {
        splittingService.putUnlessExists(TIMESTAMP_1, TIMESTAMP_2);
        splittingService.putUnlessExists(TIMESTAMP_3, TIMESTAMP_4);

        verify(service1).putUnlessExists(TIMESTAMP_1, TIMESTAMP_2);
        verify(service2).putUnlessExists(TIMESTAMP_3, TIMESTAMP_4);
    }

    @Test
    public void putUnlessExistsDelegatesBasedOnStartTimestamp() {
        splittingService.putUnlessExists(TIMESTAMP_1, TIMESTAMP_3);
        verify(service1).putUnlessExists(TIMESTAMP_1, TIMESTAMP_3);
    }

    @Test
    public void getMultipleOnlyCallsUnderlyingServicesOnce() {
        splittingService.get(ImmutableList.of(TIMESTAMP_1, TIMESTAMP_2, TIMESTAMP_3, TIMESTAMP_4));
        verify(service1).get(eq(ImmutableList.of(TIMESTAMP_1, TIMESTAMP_2)));
        verify(service2).get(eq(ImmutableList.of(TIMESTAMP_3, TIMESTAMP_4)));
    }

    @Test
    public void getMultipleOnlyCallsUnderlyingServiceIfRelevant() {
        splittingService.get(ImmutableList.of(TIMESTAMP_1, TIMESTAMP_2));
        verify(service1).get(eq(ImmutableList.of(TIMESTAMP_1, TIMESTAMP_2)));
    }

    @Test
    public void responsiveToChangesInPredicate() {
        AtomicLong bound = new AtomicLong(100);
        TransactionService dynamicSplitter = forPredicate(ts -> ts < bound.get());
        dynamicSplitter.get(TIMESTAMP_1);
        bound.set(Long.MAX_VALUE);
        dynamicSplitter.get(TIMESTAMP_4);
        verify(service1).get(TIMESTAMP_1);
        verify(service1).get(TIMESTAMP_4);
    }

    @SuppressWarnings("unchecked") // Known safe mock invocation
    @Test
    public void propagatesExceptions() {
        when(service1.get(anyLong())).thenThrow(IllegalStateException.class);
        assertThatThrownBy(() -> splittingService.get(TIMESTAMP_1))
                .isInstanceOf(IllegalStateException.class);
        verify(service1).get(TIMESTAMP_1);
    }

    private TransactionService forPredicate(LongPredicate longPredicate) {
        return new DynamicSplittingTransactionService(longPredicate, service1, service2);
    }
}
