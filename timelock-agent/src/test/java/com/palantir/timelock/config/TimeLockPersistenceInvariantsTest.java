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
package com.palantir.timelock.config;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class TimeLockPersistenceInvariantsTest {
    @Test
    public void doesNotCreateDirectoryForPaxosDirectoryIfNewService() throws IOException {
        File mockFile = getMockFileWith(false, true);

        assertCanBuildConfiguration(
                PaxosInstallConfiguration.builder().dataDirectory(mockFile).isNewService(true));

        verify(mockFile, times(0)).mkdirs();
    }

    @Test
    public void canUseExistingDirectoryAsPaxosDirectory() throws IOException {
        File mockFile = getMockFileWith(true, false);

        assertCanBuildConfiguration(
                PaxosInstallConfiguration.builder().dataDirectory(mockFile).isNewService(false));

        verify(mockFile, atLeastOnce()).isDirectory();
    }

    private File getMockFileWith(boolean isDirectory, boolean canCreateDirectory) throws IOException {
        File mockFile = mock(File.class);
        when(mockFile.mkdirs()).thenReturn(canCreateDirectory);
        when(mockFile.isDirectory()).thenReturn(isDirectory);
        when(mockFile.getPath()).thenReturn("var/data/paxos");
        when(mockFile.getCanonicalPath()).thenReturn("/var/data/paxos");
        return mockFile;
    }

    @SuppressWarnings("CheckReturnValue")
    private void assertCanBuildConfiguration(ImmutablePaxosInstallConfiguration.Builder configBuilder) {
        PaxosInstallConfiguration installConfiguration = configBuilder.build();
        TimeLockInstallConfiguration.builder().paxos(installConfiguration).build();
    }
}
