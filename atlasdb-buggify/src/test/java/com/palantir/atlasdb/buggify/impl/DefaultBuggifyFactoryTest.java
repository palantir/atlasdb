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

import org.junit.Test;

public final class DefaultBuggifyFactoryTest {

    @Test
    public void maybeReturnsDefaultWhenMaxProbability() {
        assertThat(DefaultBuggifyFactory.INSTANCE.maybe(1.0)).isSameAs(DefaultBuggify.INSTANCE);
    }

    @Test
    public void maybeReturnsNoOpWhenNeverProbability() {
        assertThat(DefaultBuggifyFactory.INSTANCE.maybe(0.0)).isSameAs(NoOpBuggify.INSTANCE);
    }

    @Test
    public void maybeReturnsNoOpWhenBelowOrEqualToProbability() {
        assertThat(new DefaultBuggifyFactory(() -> 0.50).maybe(0.50)).isSameAs(NoOpBuggify.INSTANCE);
    }

    @Test
    public void maybeReturnsDefaultWhenAboveProbability() {
        assertThat(new DefaultBuggifyFactory(() -> 0.49).maybe(0.50)).isSameAs(DefaultBuggify.INSTANCE);
    }
}
