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
package com.palantir.flake.example;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.flake.FlakeRetryingRule;
import com.palantir.flake.ShouldRetry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.junit.Rule;
import org.junit.Test;

public class StringSupplierTest {
    private static final String TEST_STRING = "123";
    private static final String EMPTY_STRING = "";

    private static final Supplier<String> FLAKY_STRING_SUPPLIER =
            () -> ThreadLocalRandom.current().nextBoolean() ? TEST_STRING : EMPTY_STRING;

    @Rule
    public FlakeRetryingRule flakeRetryingRule = new FlakeRetryingRule();

    @Test
    @ShouldRetry(numAttempts = 50) // Strobes once in a quintillion times, which I think we can live with.
    public void canGetStringFromFlakySupplier() {
        assertThat(FLAKY_STRING_SUPPLIER.get()).isEqualTo(TEST_STRING);
    }
}
