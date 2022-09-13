/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RawKeyValueServiceModuleTest {

    @Mock
    KeyValueService keyValueService;

    @Test
    public void provideInitializedRawKeyValueServiceProvidesInitializedKeyValueService() {
        when(keyValueService.isInitialized()).thenReturn(false, false, false, true);
        KeyValueService result = new RawKeyValueServiceModule().provideInitializedRawKeyValueService(keyValueService);
        assertThat(result.isInitialized()).isTrue();
    }

    @Test
    public void provideInitializedRawKeyValueServicePropagatesException() {
        when(keyValueService.isInitialized()).thenReturn(false).thenThrow(new SafeIllegalStateException("test"));
        assertThatThrownBy(() -> new RawKeyValueServiceModule().provideInitializedRawKeyValueService(keyValueService))
                .hasRootCauseInstanceOf(SafeIllegalStateException.class)
                .hasRootCauseMessage("test");
    }
}
