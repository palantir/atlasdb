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

package com.palantir.atlasdb.cassandra;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

@SuppressWarnings("ResultOfMethodCallIgnored") // We are concerned that there are no exceptions
public class CassandraCellLoadingConfigTest {
    @Test
    public void canCreateConfigWithDefaultValues() {
        assertThatCode(() -> ImmutableCassandraCellLoadingConfig.builder().build())
                .doesNotThrowAnyException();
    }

    @Test
    public void canCreateConfigWithCrossColumnBatchingDisabled() {
        assertThatCode(() -> CassandraCellLoadingConfig.of(1, 100)).doesNotThrowAnyException();
    }

    @Test
    public void cannotCreateConfigWithNonPositiveCrossColumnLoadBatchLimit() {
        assertThatThrownBy(() -> CassandraCellLoadingConfig.of(-31, 4159))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("crossColumnLoadBatchLimit should be positive");
        assertThatThrownBy(() -> CassandraCellLoadingConfig.of(0, 4242))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("crossColumnLoadBatchLimit should be positive");
    }

    @Test
    public void cannotCreateConfigWithNonPositiveSingleQueryLoadBatchLimit() {
        assertThatThrownBy(() -> CassandraCellLoadingConfig.of(5, -24))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("singleQueryLoadBatchLimit should be positive");
        assertThatThrownBy(() -> CassandraCellLoadingConfig.of(-512, -333))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("should be positive"); // could be either one
    }

    @Test
    public void cannotCreateConfigWhereCrossColumnBatchLimitExceedsSingleQueryLimit() {
        assertThatThrownBy(() -> CassandraCellLoadingConfig.of(100, 99))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("shouldn't exceed single query load batch limit");
    }

    @Test
    public void canCreateConfigWhereCrossColumnBatchLimitEqualsSingleQueryLimit() {
        assertThatCode(() -> CassandraCellLoadingConfig.of(777, 777)).doesNotThrowAnyException();
    }
}
