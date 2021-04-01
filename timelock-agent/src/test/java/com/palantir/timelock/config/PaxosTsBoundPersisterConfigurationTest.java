/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Test;

public class PaxosTsBoundPersisterConfigurationTest {
    private final TsBoundPersisterConfiguration CONFIG_ONE =
            ImmutablePaxosTsBoundPersisterConfiguration.builder().build();
    private final TsBoundPersisterConfiguration CONFIG_TWO =
            ImmutablePaxosTsBoundPersisterConfiguration.builder().build();
    private final TsBoundPersisterConfiguration ANOTHER_CONFIG = mock(TsBoundPersisterConfiguration.class);

    @Test
    public void configurationsAreReflexivelyCompatible() {
        assertThat(CONFIG_ONE.isLocationallyIncompatible(CONFIG_ONE)).isFalse();
    }

    @Test
    public void paxosConfigurationsAreMutuallyCompatible() {
        assertThat(CONFIG_ONE.isLocationallyIncompatible(CONFIG_TWO)).isFalse();
        assertThat(CONFIG_TWO.isLocationallyIncompatible(CONFIG_ONE)).isFalse();
    }

    @Test
    public void paxosConfigurationsNotCompatibleWithOtherTypes() {
        assertThat(CONFIG_ONE.isLocationallyIncompatible(ANOTHER_CONFIG)).isTrue();
        assertThat(CONFIG_TWO.isLocationallyIncompatible(ANOTHER_CONFIG)).isTrue();
    }
}