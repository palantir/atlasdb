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
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.DisabledNamespacesUpdaterService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulReenableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulReenableNamespacesResponse;
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
    private static final ImmutableSet<Namespace> BOTH_NAMESPACES = ImmutableSet.of(NAMESPACE, OTHER_NAMESPACE);
    private static final UUID LOCK_ID = new UUID(13, 37);

    private static final DisableNamespacesResponse DISABLE_FAILED_SUCCESSFULLY = DisableNamespacesResponse.unsuccessful(
            UnsuccessfulDisableNamespacesResponse.builder().build());
    private static final ReenableNamespacesResponse REENABLED_SUCCESSFULLY =
            ReenableNamespacesResponse.successful(SuccessfulReenableNamespacesResponse.of(true));

    @Mock
    private TimelockNamespaces localUpdater;

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

        when(remote1.reenable(any(), any())).thenReturn(REENABLED_SUCCESSFULLY);
        when(remote2.reenable(any(), any())).thenReturn(REENABLED_SUCCESSFULLY);
        when(localUpdater.reEnable(any())).thenReturn(REENABLED_SUCCESSFULLY);

        updater = new AllNodesDisabledNamespacesUpdater(AUTH_HEADER, remotes, executors, localUpdater, () -> LOCK_ID);
    }

    @Test
    public void canDisableSingleNamespace() {
        DisableNamespacesResponse successfulResponse = successfulDisableResponse();

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

        assertThat(response).isEqualTo(DISABLE_FAILED_SUCCESSFULLY);
        verify(remote1, never()).disable(any(), any());
        verify(remote2, never()).disable(any(), any());
    }

    // TODO(gs): test what happens if we get exception from remote.disable()
    // Case A: start with no disabled namespaces; disable fails on some node; we should re-enable all
    @Test
    public void rollsBackDisabledNamespacesAfterPartialFailure() {
        DisableNamespacesResponse successfulResponse = successfulDisableResponse();
        Set<Namespace> failedNamespaces = ImmutableSet.of(OTHER_NAMESPACE);
        DisableNamespacesResponse unsuccessfulResponse = consistentlyDisabled(failedNamespaces);

        when(remote1.disable(any(), any())).thenReturn(successfulResponse);
        when(remote2.disable(any(), any())).thenReturn(unsuccessfulResponse);
        when(localUpdater.disable(any())).thenReturn(successfulResponse);

        DisableNamespacesResponse response = updater.disableOnAllNodes(BOTH_NAMESPACES);

        assertThat(response).isEqualTo(DISABLE_FAILED_SUCCESSFULLY);
        ReenableNamespacesRequest rollbackRequest = ReenableNamespacesRequest.of(BOTH_NAMESPACES, LOCK_ID);
        verify(remote1).reenable(AUTH_HEADER, rollbackRequest);
        verify(remote2).reenable(AUTH_HEADER, rollbackRequest);
    }

    @Test
    public void rollsBackIfLocalUpdateFails() {
        DisableNamespacesResponse successfulResponse = successfulDisableResponse();

        when(remote1.disable(any(), any())).thenReturn(successfulResponse);
        when(remote2.disable(any(), any())).thenReturn(successfulResponse);

        Set<Namespace> failedNamespaces = ImmutableSet.of(OTHER_NAMESPACE);
        DisableNamespacesResponse unsuccessfulResponse = consistentlyDisabled(failedNamespaces);
        when(localUpdater.disable(DisableNamespacesRequest.of(BOTH_NAMESPACES, LOCK_ID)))
                .thenReturn(unsuccessfulResponse);

        DisableNamespacesResponse response = updater.disableOnAllNodes(BOTH_NAMESPACES);

        assertThat(response).isEqualTo(DISABLE_FAILED_SUCCESSFULLY);
        ReenableNamespacesRequest rollbackRequest = ReenableNamespacesRequest.of(BOTH_NAMESPACES, LOCK_ID);
        verify(remote1).reenable(AUTH_HEADER, rollbackRequest);
        verify(remote2).reenable(AUTH_HEADER, rollbackRequest);
        verify(localUpdater).reEnable(rollbackRequest);
    }

    // Case B: One namespace already disabled on all nodes => should not disable any namespace on any node
    //   Note that if B and C are combined, we also don't need to roll back. Some namespaces would be
    //   in an inconsistent state, but because one namespace is disabled on all nodes, we did not
    //   successfully disable any namespaces ourselves, and we therefore have nothing to roll back.
    @Test
    public void doesNotDisableIfSomeNamespaceAlreadyDisabled() {
        Set<Namespace> disabledNamespaces = ImmutableSet.of(OTHER_NAMESPACE);
        DisableNamespacesResponse unsuccessfulResponse = consistentlyDisabled(disabledNamespaces);

        when(remote1.disable(any(), any())).thenReturn(unsuccessfulResponse);
        when(remote2.disable(any(), any())).thenReturn(unsuccessfulResponse);
        when(localUpdater.disable(any())).thenReturn(unsuccessfulResponse);

        DisableNamespacesResponse response = updater.disableOnAllNodes(BOTH_NAMESPACES);

        assertThat(response).isEqualTo(unsuccessfulResponse);

        // No re-enable should take place
        verify(remote1, never()).reenable(any(), any());
        verify(remote2, never()).reenable(any(), any());
        verify(localUpdater, never()).reEnable(any());
    }

    // Case C: If we start with an inconsistent state, we roll back our request
    @Test
    public void rollsBackDisableIfInconsistentStateIsFound() {
        Set<Namespace> disabledNamespaces = ImmutableSet.of(OTHER_NAMESPACE);
        DisableNamespacesResponse unsuccessfulResponse = consistentlyDisabled(disabledNamespaces);

        when(remote1.disable(any(), any())).thenReturn(unsuccessfulResponse);
        when(remote2.disable(any(), any())).thenReturn(successfulDisableResponse());
        when(localUpdater.disable(any())).thenReturn(unsuccessfulResponse);

        DisableNamespacesResponse response = updater.disableOnAllNodes(ImmutableSet.of(NAMESPACE, OTHER_NAMESPACE));

        assertThat(response).isEqualTo(DISABLE_FAILED_SUCCESSFULLY);

        ReenableNamespacesRequest rollbackRequest = ReenableNamespacesRequest.of(BOTH_NAMESPACES, LOCK_ID);
        verify(remote1).reenable(AUTH_HEADER, rollbackRequest);
        verify(remote2).reenable(AUTH_HEADER, rollbackRequest);
        verify(localUpdater).reEnable(rollbackRequest);
    }

    @Test
    public void reportsRollbackFailures() {
        Set<Namespace> failedNamespaces = ImmutableSet.of(OTHER_NAMESPACE);
        DisableNamespacesResponse unsuccessfulResponse = consistentlyDisabled(failedNamespaces);

        when(remote1.disable(any(), any())).thenReturn(successfulDisableResponse());
        when(remote2.disable(any(), any())).thenReturn(unsuccessfulResponse);
        when(localUpdater.disable(any())).thenReturn(successfulDisableResponse());

        when(remote2.reenable(any(), any())).thenReturn(consistentlyLocked(failedNamespaces));

        DisableNamespacesResponse response = updater.disableOnAllNodes(BOTH_NAMESPACES);

        assertThat(response).isEqualTo(partiallyDisabled(BOTH_NAMESPACES));
    }

    @Test
    public void canReEnableSingleNamespace() {
        ReenableNamespacesRequest request = ReenableNamespacesRequest.of(ImmutableSet.of(NAMESPACE), LOCK_ID);
        when(remote1.reenable(AUTH_HEADER, request)).thenReturn(REENABLED_SUCCESSFULLY);
        when(remote2.reenable(AUTH_HEADER, request)).thenReturn(REENABLED_SUCCESSFULLY);
        when(localUpdater.reEnable(request)).thenReturn(REENABLED_SUCCESSFULLY);

        ReenableNamespacesResponse response = updater.reenableOnAllNodes(request);
        assertThat(response).isEqualTo(REENABLED_SUCCESSFULLY);
    }

    @Test
    public void doesNotReEnableIfPingFailsOnOneNode() {
        when(remote1.ping(AUTH_HEADER)).thenReturn(false);

        ReenableNamespacesRequest request = ReenableNamespacesRequest.of(ImmutableSet.of(NAMESPACE), LOCK_ID);
        ReenableNamespacesResponse response = updater.reenableOnAllNodes(request);

        assertThat(response).isEqualTo(consistentlyLocked(ImmutableSet.of()));
        verify(remote1, never()).reenable(any(), any());
        verify(remote2, never()).reenable(any(), any());
    }

    // Case A: Start with one disabled namespace, re-enable fails on some node, we should re-disable
    @Test
    public void rollsBackIfReEnableFails() {
        ImmutableSet<Namespace> oneNamespace = ImmutableSet.of(NAMESPACE);
        ReenableNamespacesRequest request = ReenableNamespacesRequest.of(oneNamespace, LOCK_ID);

        when(remote2.reenable(AUTH_HEADER, request)).thenReturn(consistentlyLocked(oneNamespace));

        DisableNamespacesRequest rollbackRequest = DisableNamespacesRequest.of(oneNamespace, LOCK_ID);
        when(remote1.disable(AUTH_HEADER, rollbackRequest)).thenReturn(successfulDisableResponse());
        when(remote2.disable(AUTH_HEADER, rollbackRequest)).thenReturn(successfulDisableResponse());
        when(localUpdater.disable(rollbackRequest)).thenReturn(successfulDisableResponse());

        ReenableNamespacesResponse response = updater.reenableOnAllNodes(request);
        assertThat(response).isEqualTo(consistentlyLocked(ImmutableSet.of()));

        verify(remote1).disable(AUTH_HEADER, rollbackRequest);
        verify(remote2).disable(AUTH_HEADER, rollbackRequest);
        verify(localUpdater).disable(rollbackRequest);
    }

    // Case B: re-enable with wrong lock -> don't re-enable anywhere
    @Test
    public void doesNotReEnableIfSomeNamespaceDisabledWithOtherLock() {
        Set<Namespace> disabledNamespaces = ImmutableSet.of(OTHER_NAMESPACE);
        ReenableNamespacesResponse unsuccessfulResponse = consistentlyLocked(disabledNamespaces);

        when(remote1.reenable(any(), any())).thenReturn(unsuccessfulResponse);
        when(remote2.reenable(any(), any())).thenReturn(unsuccessfulResponse);
        when(localUpdater.reEnable(any())).thenReturn(unsuccessfulResponse);

        ReenableNamespacesResponse response =
                updater.reenableOnAllNodes(ReenableNamespacesRequest.of(BOTH_NAMESPACES, LOCK_ID));

        assertThat(response).isEqualTo(consistentlyLocked(disabledNamespaces));

        // No disable should take place
        verify(remote1, never()).disable(any(), any());
        verify(remote2, never()).disable(any(), any());
        verify(localUpdater, never()).disable(any());
    }

    // Case C: re-enable but inconsistent state -> roll back
    @Test
    public void rollsBackReEnableIfInconsistentStateIsFound() {
        Set<Namespace> lockedNamespaces = ImmutableSet.of(OTHER_NAMESPACE);
        ReenableNamespacesResponse unsuccessfulResponse = consistentlyLocked(lockedNamespaces);

        when(remote1.reenable(any(), any())).thenReturn(unsuccessfulResponse);
        when(remote2.reenable(any(), any())).thenReturn(REENABLED_SUCCESSFULLY);
        when(localUpdater.reEnable(any())).thenReturn(unsuccessfulResponse);
        when(remote1.disable(any(), any())).thenReturn(successfulDisableResponse());
        when(remote2.disable(any(), any())).thenReturn(successfulDisableResponse());
        when(localUpdater.disable(any())).thenReturn(successfulDisableResponse());

        ReenableNamespacesResponse response =
                updater.reenableOnAllNodes(ReenableNamespacesRequest.of(BOTH_NAMESPACES, LOCK_ID));

        assertThat(response).isEqualTo(consistentlyLocked(ImmutableSet.of()));

        DisableNamespacesRequest rollbackRequest = DisableNamespacesRequest.of(BOTH_NAMESPACES, LOCK_ID);
        verify(remote1).disable(AUTH_HEADER, rollbackRequest);
        verify(remote2).disable(AUTH_HEADER, rollbackRequest);
        verify(localUpdater).disable(rollbackRequest);
    }

    @Test
    public void reportsReEnableRollbackFailures() {
        Set<Namespace> failedNamespaces = ImmutableSet.of(OTHER_NAMESPACE);
        ReenableNamespacesResponse unsuccessfulResponse = consistentlyLocked(failedNamespaces);

        when(remote1.reenable(any(), any())).thenReturn(unsuccessfulResponse);
        when(remote2.reenable(any(), any())).thenReturn(REENABLED_SUCCESSFULLY);
        when(localUpdater.reEnable(any())).thenReturn(unsuccessfulResponse);
        when(remote1.disable(any(), any())).thenReturn(successfulDisableResponse());
        when(remote2.disable(any(), any())).thenReturn(consistentlyDisabled(failedNamespaces));
        when(localUpdater.disable(any())).thenReturn(successfulDisableResponse());

        ReenableNamespacesResponse response =
                updater.reenableOnAllNodes(ReenableNamespacesRequest.of(BOTH_NAMESPACES, LOCK_ID));

        assertThat(response).isEqualTo(partiallyLocked(BOTH_NAMESPACES));
    }

    private static DisableNamespacesResponse successfulDisableResponse() {
        return DisableNamespacesResponse.successful(SuccessfulDisableNamespacesResponse.of(LOCK_ID));
    }

    private static DisableNamespacesResponse consistentlyDisabled(Set<Namespace> consistentlyDisabled) {
        return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.builder()
                .consistentlyDisabledNamespaces(consistentlyDisabled)
                .build());
    }

    private static DisableNamespacesResponse partiallyDisabled(Set<Namespace> partiallyDisabled) {
        return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.builder()
                .partiallyDisabledNamespaces(partiallyDisabled)
                .build());
    }

    private static ReenableNamespacesResponse consistentlyLocked(Set<Namespace> consistentlyLocked) {
        return ReenableNamespacesResponse.unsuccessful(UnsuccessfulReenableNamespacesResponse.builder()
                .consistentlyLockedNamespaces(consistentlyLocked)
                .build());
    }

    private static ReenableNamespacesResponse partiallyLocked(Set<Namespace> partiallyLocked) {
        return ReenableNamespacesResponse.unsuccessful(UnsuccessfulReenableNamespacesResponse.builder()
                .partiallyLockedNamespaces(partiallyLocked)
                .build());
    }
}
