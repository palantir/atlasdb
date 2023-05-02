/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.buggify.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.function.Function;
import org.junit.Test;

public class NoOpBuggifyTest {
    @Test
    public void runNeverExecutes() {
        Runnable runnable = mock(Runnable.class);
        NoOpBuggify.INSTANCE.run(runnable);
        verify(runnable, never()).run();
    }

    @Test
    public void mapNeverCallsFunction() {
        Function<Object, Object> value = mock(Function.class);
        Object object = new Object();
        Object mappedObject = NoOpBuggify.INSTANCE.map(object, value);
        assertThat(object).isEqualTo(mappedObject);
        verify(value, never()).apply(any());
    }
}
