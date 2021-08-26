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
package com.palantir.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import org.junit.Test;

public class OptionalResolverTest {
    private static final String STRING_1 = "a";
    private static final String STRING_2 = "b";

    private static final Optional<String> PRESENT_OPTIONAL_1 = Optional.of(STRING_1);
    private static final Optional<String> PRESENT_OPTIONAL_2 = Optional.of(STRING_2);
    private static final Optional<String> EMPTY_OPTIONAL = Optional.empty();

    @Test
    public void throwsOnTwoEmptyOptionals() {
        assertThatThrownBy(() -> OptionalResolver.resolve(EMPTY_OPTIONAL, EMPTY_OPTIONAL))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsOnTwoNullOptionals() {
        assertThatThrownBy(() -> OptionalResolver.resolve(null, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsOnOneEmptyAndOneNullOptional() {
        assertThatThrownBy(() -> OptionalResolver.resolve(null, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void resolvesToValueIfOneOptionalPresent() {
        assertThat(OptionalResolver.resolve(PRESENT_OPTIONAL_1, EMPTY_OPTIONAL)).isEqualTo(STRING_1);
        assertThat(OptionalResolver.resolve(EMPTY_OPTIONAL, PRESENT_OPTIONAL_2)).isEqualTo(STRING_2);
    }

    @Test
    public void resolvesToValueIfOneOptionalPresentAndOneIsNull() {
        assertThat(OptionalResolver.resolve(PRESENT_OPTIONAL_1, Optional.empty()))
                .isEqualTo(STRING_1);
    }

    @Test
    public void resolvesToValueOnTwoEqualOptionals() {
        assertThat(OptionalResolver.resolve(PRESENT_OPTIONAL_1, Optional.of(STRING_1)))
                .isEqualTo(STRING_1);
    }

    @Test
    public void throwsOnTwoUnequalOptionals() {
        assertThatThrownBy(() -> OptionalResolver.resolve(PRESENT_OPTIONAL_1, PRESENT_OPTIONAL_2))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
