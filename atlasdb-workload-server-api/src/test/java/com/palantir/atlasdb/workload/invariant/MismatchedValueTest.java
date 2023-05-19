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

package com.palantir.atlasdb.workload.invariant;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.palantir.logsafe.SafeArg;
import java.util.Optional;
import org.junit.Test;

public final class MismatchedValueTest {

    private static final Optional<Integer> VALUE_ONE = Optional.of(15);
    private static final Optional<Integer> VALUE_TWO = Optional.of(25);

    @Test
    public void canConstructMismatchedValueFromTwoDifferentValues() {
        assertThatCode(() -> MismatchedValue.of(VALUE_ONE, VALUE_TWO)).doesNotThrowAnyException();
    }

    @Test
    public void actualIsFirstArgumentAndExpectedIsSecondArgument() {
        MismatchedValue mismatchedValue = MismatchedValue.of(VALUE_ONE, VALUE_TWO);
        assertThat(mismatchedValue.actual()).isEqualTo(VALUE_ONE);
        assertThat(mismatchedValue.expected()).isEqualTo(VALUE_TWO);
    }

    @Test
    public void throwsWhenActualAndExpectedAreTheSame() {
        assertThatLoggableExceptionThrownBy(() -> MismatchedValue.of(VALUE_ONE, VALUE_ONE))
                .hasMessageContaining("Cannot construct MismatchedValues for two values which are equal.")
                .hasExactlyArgs(SafeArg.of("actual", VALUE_ONE), SafeArg.of("expected", VALUE_ONE));
    }
}
