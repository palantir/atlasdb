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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.TransformingCoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import java.util.Optional;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked") // This test uses mocks liberally
public class TransformingCoordinationServiceTest {
    private static final int INTEGER_1 = 1;
    private static final String STRING_1 = "1";
    private static final long BOUND = 1234567;
    private static final Function<ValueAndBound<String>, String> DUMMY_TRANSFORMATION = unused -> {
        throw new IllegalStateException("I'm not supposed to be called directly, use mocks");
    };

    private CoordinationService<Integer> delegate = mock(CoordinationService.class);
    private Function<String, Integer> stringToIntTransform = mock(Function.class);
    private Function<Integer, String> intToStringTransform = mock(Function.class);
    private CoordinationService<String> coordinationService
            = new TransformingCoordinationService<>(delegate, intToStringTransform, stringToIntTransform);

    @Before
    public void setUp() {
        when(stringToIntTransform.apply(STRING_1)).thenReturn(INTEGER_1);
        when(intToStringTransform.apply(INTEGER_1)).thenReturn(STRING_1);
    }

    @Test
    public void getsValueFromDelegateIfPresent() {
        when(delegate.getValueForTimestamp(anyLong())).thenReturn(Optional.of(ValueAndBound.of(INTEGER_1, BOUND)));

        assertThat(coordinationService.getValueForTimestamp(BOUND)).contains(ValueAndBound.of(STRING_1, BOUND));
        verify(delegate).getValueForTimestamp(BOUND);
        verify(intToStringTransform).apply(INTEGER_1);
    }

    @Test
    public void getsEmptyFromDelegateIfEmpty() {
        when(delegate.getValueForTimestamp(anyLong())).thenReturn(Optional.empty());

        assertThat(coordinationService.getValueForTimestamp(BOUND)).isEmpty();
        verify(delegate).getValueForTimestamp(BOUND);
    }

    @Test
    public void checkAndSetSuccessfulCase() {
        when(delegate.tryTransformCurrentValue(any())).thenReturn(
                CheckAndSetResult.of(true, ImmutableList.of(ValueAndBound.of(INTEGER_1, BOUND))));

        CheckAndSetResult<ValueAndBound<String>> casResult =
                coordinationService.tryTransformCurrentValue(DUMMY_TRANSFORMATION);
        assertThat(casResult.successful()).isTrue();
        assertThat(casResult.existingValues()).containsExactly(ValueAndBound.of(STRING_1, BOUND));
        verify(delegate).tryTransformCurrentValue(any());
    }

    @Test
    public void checkAndSetFailureCase() {
        when(delegate.tryTransformCurrentValue(any())).thenReturn(
                CheckAndSetResult.of(false, ImmutableList.of(ValueAndBound.of(INTEGER_1, BOUND))));

        CheckAndSetResult<ValueAndBound<String>> casResult =
                coordinationService.tryTransformCurrentValue(DUMMY_TRANSFORMATION);
        assertThat(casResult.successful()).isFalse();
        assertThat(casResult.existingValues()).containsExactly(ValueAndBound.of(STRING_1, BOUND));
        verify(delegate).tryTransformCurrentValue(any());
    }

    @Test
    public void getsLastKnownLocalValueFromDelegateIfPresent() {
        when(delegate.getLastKnownLocalValue()).thenReturn(Optional.of(ValueAndBound.of(INTEGER_1, BOUND)));

        assertThat(coordinationService.getLastKnownLocalValue()).contains(ValueAndBound.of(STRING_1, BOUND));
        verify(delegate).getLastKnownLocalValue();
        verify(intToStringTransform).apply(INTEGER_1);
    }

    @Test
    public void getsEmptyLocalValueFromDelegateIfDelegateHasEmptyLocalValue() {
        when(delegate.getLastKnownLocalValue()).thenReturn(Optional.empty());

        assertThat(coordinationService.getLastKnownLocalValue()).isEmpty();
        verify(delegate).getLastKnownLocalValue();
    }
}
