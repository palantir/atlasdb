/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Test;

public class PaxosInstallConfigurationTest {

    @Test
    public void canCreateDirectoryForPaxosDirectoryIfNewService() {
        File mockFile = getMockFileWith(false, true);

        ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(mockFile)
                .isNewService(true)
                .build();

        verify(mockFile).mkdirs();
    }

    @Test
    public void canUseExistingDirectoryAsPaxosDirectory() {
        File mockFile = getMockFileWith(true, false);

        ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(mockFile)
                .isNewService(false)
                .build();

        verify(mockFile, atLeastOnce()).isDirectory();
    }

    @Test
    public void throwsIfCannotCreatePaxosDirectory() {
        File mockFile = getMockFileWith(false, false);

        assertFailsToBuildConfiguration(ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(mockFile)
                .isNewService(true));
    }

    @Test
    public void throwsIfConfiguredToBeNewServiceWithExistingDirectory() {
        File mockFile = getMockFileWith(true, true);

        assertFailsToBuildConfiguration(ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(mockFile)
                .isNewService(true));
    }

    @Test
    public void throwsIfConfiguredToBeExistingServiceWithoutDirectory() {
        File mockFile = getMockFileWith(false, true);

        assertFailsToBuildConfiguration(ImmutablePaxosInstallConfiguration.builder()
                .dataDirectory(mockFile)
                .isNewService(false));
    }

    private File getMockFileWith(boolean isDirectory, boolean canCreateDirectory) {
        File mockFile = mock(File.class);
        when(mockFile.mkdirs()).thenReturn(canCreateDirectory);
        when(mockFile.isDirectory()).thenReturn(isDirectory);
        return mockFile;
    }

    private void assertFailsToBuildConfiguration(ImmutablePaxosInstallConfiguration.Builder configBuilder) {
        assertThatThrownBy(configBuilder::build).isInstanceOf(IllegalArgumentException.class);
    }
}
