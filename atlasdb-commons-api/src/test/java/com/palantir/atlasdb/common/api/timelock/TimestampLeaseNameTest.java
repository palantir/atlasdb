/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.common.api.timelock;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.testing.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public final class TimestampLeaseNameTest {
    @ValueSource(strings = {"ImmutableTimestamp", "ImmutableTimestampTest", "ImmutableTimestampX"})
    @ParameterizedTest
    public void throwsWhenProvidedNameStartsWithImmutableTimestamp(String name) {
        Assertions.assertThatLoggableExceptionThrownBy(() -> TimestampLeaseName.of(name))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Name must not be a reserved name")
                .hasExactlyArgs(SafeArg.of("name", name));
    }

    @ValueSource(strings = {"NotImmutableTimestamp", "CommitImmutableTimestamp", "Unrelated"})
    @ParameterizedTest
    public void doesNotThrowWhenProvidedNameDoesNotStartWithImmutableTimestamp(String name) {
        Assertions.assertThatCode(() -> TimestampLeaseName.of(name)).doesNotThrowAnyException();
    }
}
