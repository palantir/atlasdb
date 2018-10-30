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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.CoordinationServiceImpl;
import com.palantir.atlasdb.coordination.CoordinationStore;
import com.palantir.atlasdb.coordination.ImmutableValueAndBound;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.ImmutableCheckAndSetResult;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

public class CoordinationServiceImplTest {
    private static final ImmutableValueAndBound<String> STRING_AND_ONE_HUNDRED
            = ImmutableValueAndBound.of(Optional.of("string"), 100);
    private static final ImmutableValueAndBound<String> OTHER_STRING_AND_ONE_THOUSAND
            = ImmutableValueAndBound.of(Optional.of("otherstring"), 1000);
    private static final ImmutableValueAndBound<String> ANOTHER_STRING_AND_ONE_HUNDRED
            = ImmutableValueAndBound.of(Optional.of("anotherstring"), 100);

    private final AtomicLong timestamp = new AtomicLong();
    private final KeyValueService kvs = new InMemoryKeyValueService(true);
    private final CoordinationStore coordinationStore = spy(KeyValueServiceCoordinationStore.create(
            kvs, PtBytes.toBytes("coordination")));
    private final CoordinationService<String> stringCoordinationService
            = new CoordinationServiceImpl<>(coordinationStore, String.class, timestamp::incrementAndGet);

    @Test
    public void getValueWithNoValuesStoredReturnsEmpty() {
        assertThat(stringCoordinationService.getValueForTimestamp(42)).isEmpty();
    }

    @Test
    public void canStoreAndRetrieveValues() {
        stringCoordinationService.tryTransformCurrentValue(unused -> STRING_AND_ONE_HUNDRED);
        assertThat(stringCoordinationService.getValueForTimestamp(42)).contains(STRING_AND_ONE_HUNDRED);
    }

    @Test
    public void getValueForWhichNoRecentValueExistsReturnsEmpty() {
        stringCoordinationService.tryTransformCurrentValue(unused -> STRING_AND_ONE_HUNDRED);
        assertThat(stringCoordinationService.getValueForTimestamp(142)).isEmpty();
    }

    @Test
    public void canCacheValues() {
        stringCoordinationService.tryTransformCurrentValue(unused -> STRING_AND_ONE_HUNDRED);
        assertThat(stringCoordinationService.getValueForTimestamp(42)).contains(STRING_AND_ONE_HUNDRED);
        assertThat(stringCoordinationService.getValueForTimestamp(42)).contains(STRING_AND_ONE_HUNDRED);
        assertThat(stringCoordinationService.getValueForTimestamp(42)).contains(STRING_AND_ONE_HUNDRED);
        verify(coordinationStore, times(1)).getValue(anyLong());
    }

    @Test
    public void canLookUpNewValueIfCacheOutOfDate() {
        stringCoordinationService.tryTransformCurrentValue(unused -> STRING_AND_ONE_HUNDRED);
        assertThat(stringCoordinationService.getValueForTimestamp(42)).contains(STRING_AND_ONE_HUNDRED);
        assertThat(stringCoordinationService.getValueForTimestamp(742)).isEmpty();
        stringCoordinationService.tryTransformCurrentValue(unused -> OTHER_STRING_AND_ONE_THOUSAND);
        assertThat(stringCoordinationService.getValueForTimestamp(742)).contains(OTHER_STRING_AND_ONE_THOUSAND);
    }

    @Test
    public void tryTransformReturnsTrueIfChangeWasApplied() {
        assertThat(stringCoordinationService.tryTransformCurrentValue(unused -> STRING_AND_ONE_HUNDRED))
                .isTrue();
    }

    @Test
    public void tryTransformThrowsIfValuePutIsEmpty() {
        assertThatThrownBy(() -> stringCoordinationService.tryTransformCurrentValue(
                unused -> ImmutableValueAndBound.of(Optional.empty(), 12345678)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot put an empty value");
    }

    @Test
    public void tryTransformThrowsIfChangeWasNotApplied() {
        CoordinationStore casFailingStore = mock(CoordinationStore.class);
        when(casFailingStore.checkAndSetCoordinationValue(any(), any())).thenReturn(
                ImmutableCheckAndSetResult.of(false, ImmutableList.of()));

        CoordinationService<String> testCoordinationService
                = new CoordinationServiceImpl<>(casFailingStore, String.class, timestamp::incrementAndGet);
        assertThat(testCoordinationService.tryTransformCurrentValue(unused -> STRING_AND_ONE_HUNDRED))
                .isFalse();
    }

    @Test
    public void tryTransformDoesNotApplyTransformIfBoundDoesNotAdvance() {
        assertThat(stringCoordinationService.tryTransformCurrentValue(unused -> STRING_AND_ONE_HUNDRED))
                .isTrue();
        assertThat(stringCoordinationService.tryTransformCurrentValue(unused -> ANOTHER_STRING_AND_ONE_HUNDRED))
                .isFalse();
        assertThat(stringCoordinationService.getValueForTimestamp(100)).contains(STRING_AND_ONE_HUNDRED);
    }
}
