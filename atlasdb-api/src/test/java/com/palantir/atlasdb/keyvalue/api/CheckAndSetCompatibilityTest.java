/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;
import org.junit.Test;

public class CheckAndSetCompatibilityTest {
    @Test
    public void minThrowsExceptionIfNoCompatibilitiesProvided() {
        assertThatThrownBy(this::getMinCompatibility)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("min requires at least 1 element");
    }

    @Test
    public void minReturnsNotSupportedIfOneKvsHasNotSupported() {
        assertThat(getMinCompatibility(
                        CheckAndSetCompatibility.SUPPORTED_DETAIL_ON_FAILURE_MAY_PARTIALLY_PERSIST,
                        CheckAndSetCompatibility.NOT_SUPPORTED,
                        CheckAndSetCompatibility.SUPPORTED_NO_DETAIL_ON_FAILURE))
                .isEqualTo(CheckAndSetCompatibility.NOT_SUPPORTED);
    }

    @Test
    public void minReturnsSupportedNoDetailIfNotSupportedAbsent() {
        assertThat(getMinCompatibility(
                        CheckAndSetCompatibility.SUPPORTED_DETAIL_ON_FAILURE_MAY_PARTIALLY_PERSIST,
                        CheckAndSetCompatibility.SUPPORTED_DETAIL_ON_FAILURE_MAY_PARTIALLY_PERSIST,
                        CheckAndSetCompatibility.SUPPORTED_NO_DETAIL_ON_FAILURE))
                .isEqualTo(CheckAndSetCompatibility.SUPPORTED_NO_DETAIL_ON_FAILURE);
    }

    @Test
    public void minReturnsSupportedWithDetailIfAllAreSupportedWithDetail() {
        assertThat(getMinCompatibility(
                        CheckAndSetCompatibility.SUPPORTED_DETAIL_ON_FAILURE_MAY_PARTIALLY_PERSIST,
                        CheckAndSetCompatibility.SUPPORTED_DETAIL_ON_FAILURE_MAY_PARTIALLY_PERSIST,
                        CheckAndSetCompatibility.SUPPORTED_DETAIL_ON_FAILURE_MAY_PARTIALLY_PERSIST))
                .isEqualTo(CheckAndSetCompatibility.SUPPORTED_DETAIL_ON_FAILURE_MAY_PARTIALLY_PERSIST);
    }

    private CheckAndSetCompatibility getMinCompatibility(CheckAndSetCompatibility... compatibilities) {
        return CheckAndSetCompatibility.min(Stream.of(compatibilities));
    }
}
