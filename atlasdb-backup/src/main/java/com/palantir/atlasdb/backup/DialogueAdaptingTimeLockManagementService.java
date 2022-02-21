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

package com.palantir.atlasdb.backup;

import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.management.TimeLockManagementService;
import com.palantir.atlasdb.timelock.api.management.TimeLockManagementServiceBlocking;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Set;
import java.util.UUID;

public class DialogueAdaptingTimeLockManagementService implements TimeLockManagementService {
    private final TimeLockManagementServiceBlocking dialogueDelegate;

    public DialogueAdaptingTimeLockManagementService(TimeLockManagementServiceBlocking dialogueDelegate) {
        this.dialogueDelegate = dialogueDelegate;
    }

    @Override
    public Set<String> getNamespaces(AuthHeader authHeader) {
        return dialogueDelegate.getNamespaces(authHeader);
    }

    @Override
    public void achieveConsensus(AuthHeader authHeader, Set<String> namespaces) {
        dialogueDelegate.achieveConsensus(authHeader, namespaces);
    }

    @Override
    public void invalidateResources(AuthHeader authHeader, Set<String> namespaces) {
        dialogueDelegate.invalidateResources(authHeader, namespaces);
    }

    @Override
    public DisableNamespacesResponse disableTimelock(AuthHeader authHeader, DisableNamespacesRequest request) {
        return dialogueDelegate.disableTimelock(authHeader, request);
    }

    @Override
    public ReenableNamespacesResponse reenableTimelock(AuthHeader authHeader, ReenableNamespacesRequest request) {
        return dialogueDelegate.reenableTimelock(authHeader, request);
    }

    @Override
    public UUID getServerLifecycleId(AuthHeader authHeader) {
        return dialogueDelegate.getServerLifecycleId(authHeader);
    }

    @Override
    public UUID forceKillTimeLockServer(AuthHeader authHeader) {
        return dialogueDelegate.forceKillTimeLockServer(authHeader);
    }
}
