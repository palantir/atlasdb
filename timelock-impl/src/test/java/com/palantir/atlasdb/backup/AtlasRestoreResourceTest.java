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

package com.palantir.atlasdb.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.backup.api.CompleteRestoreRequest;
import com.palantir.atlasdb.backup.api.CompleteRestoreResponse;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.util.TimelockTestUtils;
import com.palantir.tokens.auth.AuthHeader;
import java.net.URL;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AtlasRestoreResourceTest {
    private static final int REMOTE_PORT = 4321;
    private static final URL LOCAL = TimelockTestUtils.url("https://localhost:1234");
    private static final URL REMOTE = TimelockTestUtils.url("https://localhost:" + REMOTE_PORT);
    private static final RedirectRetryTargeter TARGETER = RedirectRetryTargeter.create(LOCAL, List.of(LOCAL, REMOTE));

    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("header");
    private static final Namespace NAMESPACE = Namespace.of("test");
    private static final Namespace OTHER_NAMESPACE = Namespace.of("other");
    private static final long FAST_FORWARD_TIMESTAMP = 9000L;

    @Mock
    private AsyncTimelockService mockTimelock;

    @Mock
    private AsyncTimelockService otherTimelock;

    private final AtlasRestoreResource atlasRestoreResource =
            new AtlasRestoreResource(TARGETER, str -> str.equals("test") ? mockTimelock : otherTimelock);

    @Before
    public void before() {
        when(mockTimelock.getFreshTimestamp()).thenReturn(1337L);
    }

    @Test
    public void completesRestoreSuccessfully() {
        CompletedBackup completedBackup = completedBackup(NAMESPACE);
        CompleteRestoreResponse response = AtlasFutures.getUnchecked(atlasRestoreResource.completeRestore(
                AUTH_HEADER, CompleteRestoreRequest.of(ImmutableSet.of(completedBackup))));

        assertThat(response.getSuccessfulNamespaces()).containsExactly(NAMESPACE);
        verify(mockTimelock).fastForwardTimestamp(completedBackup.getBackupEndTimestamp());
    }

    @Test
    public void completeRestoreFiltersOutUnsuccessfulNamespaces() {
        when(otherTimelock.getFreshTimestamp()).thenReturn(FAST_FORWARD_TIMESTAMP + 1);

        Set<CompletedBackup> completedBackups =
                ImmutableSet.of(completedBackup(NAMESPACE), completedBackup(OTHER_NAMESPACE));

        CompleteRestoreResponse response = AtlasFutures.getUnchecked(
                atlasRestoreResource.completeRestore(AUTH_HEADER, CompleteRestoreRequest.of(completedBackups)));

        assertThat(response.getSuccessfulNamespaces()).containsExactly(NAMESPACE);
        verify(mockTimelock).fastForwardTimestamp(FAST_FORWARD_TIMESTAMP);
        verify(otherTimelock, never()).fastForwardTimestamp(anyLong());
    }

    private static CompletedBackup completedBackup(Namespace namespace) {
        return CompletedBackup.builder()
                .namespace(namespace)
                .backupStartTimestamp(1337L)
                .backupEndTimestamp(FAST_FORWARD_TIMESTAMP)
                .build();
    }
}
