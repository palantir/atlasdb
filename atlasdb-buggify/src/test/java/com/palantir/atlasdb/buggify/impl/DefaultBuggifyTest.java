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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public final class DefaultBuggifyTest {
    @Test
    public void runAlwaysExecutes() {
        Runnable runnable = mock(Runnable.class);
        DefaultBuggify.INSTANCE.run(runnable);
        verify(runnable).run();
    }

    @Test
    public void asBooleanReturnsTrue() {
        assertThat(DefaultBuggify.INSTANCE.asBoolean()).isTrue();
    }
}
