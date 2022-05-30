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

package com.palantir.atlasdb.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.util.function.Function;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RefreshableWithInitialDefaultTest {

    @Mock
    Function<Integer, String> function;

    @Test
    public void preservesValidInitialValueIfFollowingValueIsNotValid() {
        SettableRefreshable<Integer> underlyingRefreshable = Refreshable.create(1);
        String expectedValue = "hello";
        when(function.apply(1)).thenReturn(expectedValue);
        when(function.apply(2)).thenThrow(new SafeIllegalArgumentException());

        RefreshableWithInitialDefault<String> refreshable =
                RefreshableWithInitialDefault.of(underlyingRefreshable, function, "bye");
        verify(function).apply(1);
        String initial = refreshable.get();

        underlyingRefreshable.update(2);
        String updated = refreshable.get();

        assertThat(initial).describedAs("First returned value should be valid").isEqualTo(expectedValue);
        assertThat(updated)
                .describedAs("Second returned value should be ignored")
                .isEqualTo(expectedValue);
    }

    @Test
    public void firstValueInvalidShouldResolveToDefault() {
        SettableRefreshable<Integer> underlyingRefreshable = Refreshable.create(1);
        String expectedValue = "hello";
        when(function.apply(1)).thenThrow();

        RefreshableWithInitialDefault<String> refreshable =
                RefreshableWithInitialDefault.of(underlyingRefreshable, function, expectedValue);

        assertThat(refreshable.get())
                .describedAs("First invalid value should resolve to default")
                .isEqualTo(expectedValue);
    }

    @Test
    public void multipleUpdatesArePropagatedCorrectly() {
        SettableRefreshable<Integer> underlyingRefreshable = Refreshable.create(1);
        String expectedValue1 = "hello";
        String expectedValue2 = "world";
        when(function.apply(1)).thenReturn(expectedValue1);
        when(function.apply(2)).thenReturn(expectedValue2);

        RefreshableWithInitialDefault<String> refreshable =
                RefreshableWithInitialDefault.of(underlyingRefreshable, function, "bye");

        String initial = refreshable.get();

        underlyingRefreshable.update(2);
        String updated = refreshable.get();

        assertThat(initial).isEqualTo(expectedValue1);
        assertThat(updated).isEqualTo(expectedValue2);
    }
}
