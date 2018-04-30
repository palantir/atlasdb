/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Test;

public class PaxosInstallConfigurationTest {
    @Test
    public void canCreateWithDefaultValues() {
        PaxosInstallConfiguration defaultConfiguration = ImmutablePaxosInstallConfiguration.builder().build();
        assertThat(defaultConfiguration).isNotNull();
    }

    @Test
    public void canCreateDirectoryForPaxosDirectory() {
        File mockFile = mock(File.class);
        when(mockFile.mkdirs()).thenReturn(true);
        when(mockFile.isDirectory()).thenReturn(false);
        ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(mockFile)
                .build();

        //noinspection ResultOfMethodCallIgnored - the file is just a mock
        verify(mockFile, times(1)).mkdirs();
    }

    @Test
    public void canUseExistingDirectoryAsPaxosDirectory() {
        File mockFile = mock(File.class);
        when(mockFile.mkdirs()).thenReturn(false);
        when(mockFile.isDirectory()).thenReturn(true);
        ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(mockFile)
                .build();

        //noinspection ResultOfMethodCallIgnored - the file is just a mock
        verify(mockFile, times(1)).isDirectory();
    }

    @Test
    public void throwsIfCannotCreatePaxosDirectory() {
        File mockFile = mock(File.class);
        when(mockFile.mkdirs()).thenReturn(false);
        when(mockFile.isDirectory()).thenReturn(false);
        assertThatThrownBy(ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(mockFile)
                ::build).isInstanceOf(IllegalArgumentException.class);
    }
}
