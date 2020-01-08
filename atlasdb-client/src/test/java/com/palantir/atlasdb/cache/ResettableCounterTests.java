/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.cache;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public final class ResettableCounterTests {

    @Test
    public void initialState() {
        assertThat(new ResettableCounter().getValue()).isZero();
    }

    @Test
    public void testIncrement() {
        ResettableCounter resettableCounter = new ResettableCounter();

        resettableCounter.inc(1);
        assertThat(resettableCounter.getValue()).isOne();

        resettableCounter.inc(10);
        assertThat(resettableCounter.getValue()).isEqualTo(11);
    }

    @Test
    public void testReset() {
        ResettableCounter resettableCounter = new ResettableCounter();

        resettableCounter.inc(1);
        assertThat(resettableCounter.getValue()).isOne();

        resettableCounter.reset();
        assertThat(resettableCounter.getValue()).isZero();
    }
}
