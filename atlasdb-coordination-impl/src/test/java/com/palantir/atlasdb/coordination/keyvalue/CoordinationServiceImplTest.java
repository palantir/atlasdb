/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.coordination.keyvalue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.CoordinationServiceImpl;
import com.palantir.atlasdb.coordination.CoordinationStore;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.common.concurrent.PTExecutors;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class CoordinationServiceImplTest {
    private static final String STRING = "string";
    private static final ValueAndBound<String> STRING_AND_ONE_HUNDRED = ValueAndBound.of(Optional.of(STRING), 100);
    private static final ValueAndBound<String> OTHER_STRING_AND_ONE_THOUSAND =
            ValueAndBound.of(Optional.of("otherstring"), 1000);
    private static final ValueAndBound<String> ANOTHER_STRING_AND_ONE_HUNDRED =
            ValueAndBound.of(Optional.of("anotherstring"), 100);

    @SuppressWarnings("unchecked") // Known to be safe in context of this test.
    private final CoordinationStore<String> coordinationStore = mock(CoordinationStore.class);

    private final CoordinationService<String> stringCoordinationService =
            new CoordinationServiceImpl<>(coordinationStore);

    @Test
    public void getValueWithNoValuesStoredReturnsEmpty() {
        when(coordinationStore.getAgreedValue()).thenReturn(Optional.empty());
        assertThat(stringCoordinationService.getValueForTimestamp(42)).isEmpty();
    }

    @Test
    public void canRetrieveValues() {
        when(coordinationStore.getAgreedValue()).thenReturn(Optional.of(STRING_AND_ONE_HUNDRED));
        assertThat(stringCoordinationService.getValueForTimestamp(42)).contains(STRING_AND_ONE_HUNDRED);
    }

    @Test
    public void getValueForWhichNoRecentValueExistsReturnsEmpty() {
        when(coordinationStore.getAgreedValue()).thenReturn(Optional.of(STRING_AND_ONE_HUNDRED));
        assertThat(stringCoordinationService.getValueForTimestamp(142)).isEmpty();
    }

    @Test
    public void canCacheValues() {
        when(coordinationStore.getAgreedValue()).thenReturn(Optional.of(STRING_AND_ONE_HUNDRED));
        assertThat(stringCoordinationService.getValueForTimestamp(42)).contains(STRING_AND_ONE_HUNDRED);
        assertThat(stringCoordinationService.getValueForTimestamp(42)).contains(STRING_AND_ONE_HUNDRED);
        assertThat(stringCoordinationService.getValueForTimestamp(42)).contains(STRING_AND_ONE_HUNDRED);
        verify(coordinationStore, times(1)).getAgreedValue();
    }

    @Test
    public void canLookUpNewValueIfCacheOutOfDate() {
        when(coordinationStore.getAgreedValue()).thenReturn(Optional.of(STRING_AND_ONE_HUNDRED));
        assertThat(stringCoordinationService.getValueForTimestamp(42)).contains(STRING_AND_ONE_HUNDRED);
        assertThat(stringCoordinationService.getValueForTimestamp(742)).isEmpty();
        when(coordinationStore.getAgreedValue()).thenReturn(Optional.of(OTHER_STRING_AND_ONE_THOUSAND));
        assertThat(stringCoordinationService.getValueForTimestamp(742)).contains(OTHER_STRING_AND_ONE_THOUSAND);
    }

    @Test
    public void delegatesTransformationToStore() {
        when(coordinationStore.transformAgreedValue(any()))
                .thenReturn(CheckAndSetResult.of(true, ImmutableList.of(STRING_AND_ONE_HUNDRED)));
        CheckAndSetResult<ValueAndBound<String>> casResult =
                stringCoordinationService.tryTransformCurrentValue(unused -> STRING);
        assertThat(casResult.successful()).isTrue();
        assertThat(Iterables.getOnlyElement(casResult.existingValues())).isEqualTo(STRING_AND_ONE_HUNDRED);
    }

    @Test
    public void successfulUpdateUpdatesCache() {
        when(coordinationStore.transformAgreedValue(any()))
                .thenReturn(CheckAndSetResult.of(true, ImmutableList.of(STRING_AND_ONE_HUNDRED)));
        stringCoordinationService.tryTransformCurrentValue(unused -> STRING);
        stringCoordinationService.getValueForTimestamp(56);
        stringCoordinationService.getValueForTimestamp(88);
        verify(coordinationStore, never()).getAgreedValue();
    }

    @Test
    public void failedUpdateUpdatesCache() {
        when(coordinationStore.transformAgreedValue(any()))
                .thenReturn(CheckAndSetResult.of(false, ImmutableList.of(STRING_AND_ONE_HUNDRED)));
        stringCoordinationService.tryTransformCurrentValue(unused -> STRING);
        stringCoordinationService.getValueForTimestamp(56);
        stringCoordinationService.getValueForTimestamp(88);
        verify(coordinationStore, never()).getAgreedValue();
    }

    @Test
    public void retrieveLocalValueRetrievesCachedValue() {
        when(coordinationStore.getAgreedValue()).thenReturn(Optional.of(STRING_AND_ONE_HUNDRED));
        assertThat(stringCoordinationService.getValueForTimestamp(42)).contains(STRING_AND_ONE_HUNDRED);
        when(coordinationStore.getAgreedValue()).thenReturn(Optional.of(OTHER_STRING_AND_ONE_THOUSAND));
        assertThat(stringCoordinationService.getLastKnownLocalValue()).contains(STRING_AND_ONE_HUNDRED);
        verify(coordinationStore, times(1)).getAgreedValue();
    }

    @Test
    public void retrieveLocalValueReturnsEmptyIfNothingCached() {
        when(coordinationStore.getAgreedValue()).thenReturn(Optional.of(OTHER_STRING_AND_ONE_THOUSAND));
        assertThat(stringCoordinationService.getLastKnownLocalValue()).isEmpty();
        verify(coordinationStore, never()).getAgreedValue();
    }

    @Test
    public void callsToGetAgreedValuesAreCoalesced() throws InterruptedException {
        CountDownLatch canStartMakingNonBlockingRequests = new CountDownLatch(1);
        CountDownLatch nonBlockingRequestsDone = new CountDownLatch(1);
        when(coordinationStore.getAgreedValue()).thenAnswer(invocation -> {
            canStartMakingNonBlockingRequests.countDown();
            nonBlockingRequestsDone.await();
            return Optional.of(STRING_AND_ONE_HUNDRED);
        });

        ExecutorService blockedExecutor = PTExecutors.newSingleThreadExecutor();
        blockedExecutor.submit(() -> stringCoordinationService.getValueForTimestamp(42));

        canStartMakingNonBlockingRequests.await();
        ExecutorService executorService = PTExecutors.newFixedThreadPool(100);
        for (int i = 0; i < 10_000; i++) {
            executorService.submit(() -> stringCoordinationService.getValueForTimestamp(42));
        }
        executorService.shutdownNow();
        assertThat(executorService.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        nonBlockingRequestsDone.countDown();

        blockedExecutor.shutdownNow();
        assertThat(blockedExecutor.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        verify(coordinationStore, times(1)).getAgreedValue();
    }
}
