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

package com.palantir.atlasdb.timelock.management;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.DisabledNamespacesUpdaterService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulDisableNamespacesResponse;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.tokens.auth.AuthHeader;
import com.palantir.tokens.auth.BearerToken;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class AllNodesDisabledNamespacesUpdaterTest {
    private static final BearerToken BEARER_TOKEN = BearerToken.valueOf("bear");
    private static final AuthHeader AUTH_HEADER = AuthHeader.of(BEARER_TOKEN);
    private static final Namespace NAMESPACE = Namespace.of("namespace");
    private static final Namespace OTHER_NAMESPACE = Namespace.of("other-namespace");
    private static final UUID LOCK_ID = new UUID(13, 37);
    private static final DisableNamespacesResponse FAILED_SUCCESSFULLY =
            DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.of(ImmutableSet.of()));

    @Mock
    private DisabledNamespaces localUpdater;

    @Mock
    private DisabledNamespacesUpdaterService remote1;

    @Mock
    private DisabledNamespacesUpdaterService remote2;

    private AllNodesDisabledNamespacesUpdater updater;

    @Before
    public void setUp() {
        when(remote1.ping(AUTH_HEADER)).thenReturn(true);
        when(remote2.ping(AUTH_HEADER)).thenReturn(true);

        ImmutableList<DisabledNamespacesUpdaterService> remotes = ImmutableList.of(remote1, remote2);
        Map<DisabledNamespacesUpdaterService, CheckedRejectionExecutorService> executors = ImmutableMap.of(
                remote1, new CheckedRejectionExecutorService(Executors.newSingleThreadExecutor()),
                remote2, new CheckedRejectionExecutorService(Executors.newSingleThreadExecutor()));

        updater = new AllNodesDisabledNamespacesUpdater(AUTH_HEADER, remotes, executors, localUpdater, () -> LOCK_ID);
    }

    @Test
    public void canDisableSingleNamespace() {
        DisableNamespacesResponse successfulResponse =
                DisableNamespacesResponse.successful(SuccessfulDisableNamespacesResponse.of(LOCK_ID));

        when(remote1.disable(any(), any())).thenReturn(successfulResponse);
        when(remote2.disable(any(), any())).thenReturn(successfulResponse);
        when(localUpdater.disable(any(DisableNamespacesRequest.class))).thenReturn(successfulResponse);

        DisableNamespacesResponse response = updater.disableOnAllNodes(ImmutableSet.of(NAMESPACE));

        assertThat(response).isEqualTo(successfulResponse);
    }

    @Test
    public void doesNotDisableIfPingFailsOnOneNode() {
        when(remote2.ping(AUTH_HEADER)).thenReturn(false);

        DisableNamespacesResponse response = updater.disableOnAllNodes(ImmutableSet.of(NAMESPACE));

        assertThat(response).isEqualTo(FAILED_SUCCESSFULLY);
        verify(remote1, never()).disable(any(), any());
        verify(remote2, never()).disable(any(), any());
    }

    @Test
    public void rollsBackDisabledNamespacesAfterPartialFailure() {
        DisableNamespacesResponse successfulResponse =
                DisableNamespacesResponse.successful(SuccessfulDisableNamespacesResponse.of(LOCK_ID));
        Set<Namespace> failedNamespaces = ImmutableSet.of(OTHER_NAMESPACE);
        DisableNamespacesResponse unsuccessfulResponse =
                DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.of(failedNamespaces));

        when(remote1.disable(any(), any())).thenReturn(successfulResponse);
        when(remote2.disable(any(), any())).thenReturn(unsuccessfulResponse);

        DisableNamespacesResponse response = updater.disableOnAllNodes(ImmutableSet.of(NAMESPACE, OTHER_NAMESPACE));

        assertThat(response).isEqualTo(FAILED_SUCCESSFULLY);
        ReenableNamespacesRequest rollbackRequest = ReenableNamespacesRequest.of(failedNamespaces, LOCK_ID);
        verify(remote1).reenable(AUTH_HEADER, rollbackRequest);
        verify(remote2).reenable(AUTH_HEADER, rollbackRequest);
    }

    @Test
    public void rollsBackIfLocalUpdateFails() {
        DisableNamespacesResponse successfulResponse =
                DisableNamespacesResponse.successful(SuccessfulDisableNamespacesResponse.of(LOCK_ID));

        when(remote1.disable(any(), any())).thenReturn(successfulResponse);
        when(remote2.disable(any(), any())).thenReturn(successfulResponse);

        Set<Namespace> namespaces = ImmutableSet.of(NAMESPACE, OTHER_NAMESPACE);
        Set<Namespace> failedNamespaces = ImmutableSet.of(OTHER_NAMESPACE);
        DisableNamespacesResponse unsuccessfulResponse =
                DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.of(failedNamespaces));
        when(localUpdater.disable(DisableNamespacesRequest.of(namespaces, LOCK_ID)))
                .thenReturn(unsuccessfulResponse);

        DisableNamespacesResponse response = updater.disableOnAllNodes(ImmutableSet.of(NAMESPACE, OTHER_NAMESPACE));

        assertThat(response).isEqualTo(FAILED_SUCCESSFULLY);
        ReenableNamespacesRequest rollbackRequest = ReenableNamespacesRequest.of(failedNamespaces, LOCK_ID);
        verify(remote1).reenable(AUTH_HEADER, rollbackRequest);
        verify(remote2).reenable(AUTH_HEADER, rollbackRequest);
        verify(localUpdater).reEnable(rollbackRequest);
    }

    @Test
    public void canReEnableSingleNamespace() {
        ReenableNamespacesResponse successfulResponse = ReenableNamespacesResponse.of(true);

        ReenableNamespacesRequest request = ReenableNamespacesRequest.of(ImmutableSet.of(NAMESPACE), LOCK_ID);
        when(remote1.reenable(AUTH_HEADER, request)).thenReturn(successfulResponse);
        when(remote2.reenable(AUTH_HEADER, request)).thenReturn(successfulResponse);
        when(localUpdater.reEnable(request)).thenReturn(successfulResponse);

        ReenableNamespacesResponse response = updater.reenableOnAllNodes(request);
        assertThat(response).isEqualTo(successfulResponse);
    }

    @Test
    public void doesNotReEnableIfPingFailsOnOneNode() {
        when(remote1.ping(AUTH_HEADER)).thenReturn(false);

        ReenableNamespacesRequest request = ReenableNamespacesRequest.of(ImmutableSet.of(NAMESPACE), LOCK_ID);
        ReenableNamespacesResponse response = updater.reenableOnAllNodes(request);

        assertThat(response).isEqualTo(ReenableNamespacesResponse.of(false));
        verify(remote1, never()).reenable(any(), any());
        verify(remote2, never()).reenable(any(), any());
    }
}
