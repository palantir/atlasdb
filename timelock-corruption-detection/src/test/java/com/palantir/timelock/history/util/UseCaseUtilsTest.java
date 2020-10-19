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

package com.palantir.timelock.history.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import org.junit.Test;

public class UseCaseUtilsTest {
    @Test
    public void throwsOnInvalidFormatUseCases() {
        assertHasInvalidFormat("abcdefg");
        assertHasInvalidFormat("");
    }

    @Test
    public void parsesSimpleUseCases() {
        assertThat(UseCaseUtils.getPaxosUseCasePrefix("tom!acceptor")).isEqualTo("tom");
        assertThat(UseCaseUtils.getPaxosUseCasePrefix("tom!learner")).isEqualTo("tom");
    }

    @Test
    public void parsesUseCasesWithMultipleDelimiters() {
        assertThat(UseCaseUtils.getPaxosUseCasePrefix("a!b!c!d")).isEqualTo("a!b!c");
        assertThat(UseCaseUtils.getPaxosUseCasePrefix("!!!!!!!")).isEqualTo("!!!!!!");
    }

    private void assertHasInvalidFormat(String candidateUseCase) {
        assertThatThrownBy(() -> UseCaseUtils.getPaxosUseCasePrefix(candidateUseCase))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Unexpected use case format");
    }
}
