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

package com.palantir.atlasdb.keyvalue.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import org.junit.Test;

public class DualWriteKeyValueServiceTest {
    private final KeyValueService delegate1 = mock(KeyValueService.class);
    private final KeyValueService delegate2 = mock(KeyValueService.class);

    private final KeyValueService dualWriteService = new DualWriteKeyValueService(delegate1, delegate2);

    @Test
    public void checkAndSetCompatibilityIsBasedOnTheFirstDelegate() {
        when(delegate1.getCheckAndSetCompatibility())
                .thenReturn(CheckAndSetCompatibility.SUPPORTS_DETAIL_NOT_CONSISTENT_ON_FAILURE);
        when(delegate2.getCheckAndSetCompatibility()).thenReturn(CheckAndSetCompatibility.NO_DETAIL_CONSISTENT_ON_FAILURE);

        assertThat(dualWriteService.getCheckAndSetCompatibility())
                .isEqualTo(CheckAndSetCompatibility.SUPPORTS_DETAIL_NOT_CONSISTENT_ON_FAILURE);
        verify(delegate1).getCheckAndSetCompatibility();
        verifyNoMoreInteractions(delegate1, delegate2);
    }
}
