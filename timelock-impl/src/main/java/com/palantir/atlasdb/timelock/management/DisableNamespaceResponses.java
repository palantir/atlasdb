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

import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.UnsuccessfulDisableNamespacesResponse;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Set;
import java.util.UUID;

final class DisableNamespaceResponses {
    private static final SafeLogger log = SafeLoggerFactory.get(DisableNamespaceResponses.class);

    private DisableNamespaceResponses() {
        // utility
    }

    static DisableNamespacesResponse unsuccessfulDueToPingFailure(Set<Namespace> namespaces) {
        log.error(
                "Failed to reach all remote nodes. Not disabling any namespaces", SafeArg.of("namespaces", namespaces));
        return DisableNamespacesResponse.unsuccessful(
                UnsuccessfulDisableNamespacesResponse.builder().build());
    }

    static DisableNamespacesResponse unsuccessfulDueToConsistentlyLockedNamespaces(
            Set<Namespace> consistentlyDisabledNamespaces) {
        log.error(
                "Failed to disable all namespaces, because some namespace was consistently disabled. This implies"
                        + " that this namespace is already being restored. If that is the case, please either wait for"
                        + " that restore to complete, or kick off a restore without that namespace",
                SafeArg.of("disabledNamespaces", consistentlyDisabledNamespaces));
        return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.builder()
                .consistentlyDisabledNamespaces(consistentlyDisabledNamespaces)
                .build());
    }

    static DisableNamespacesResponse unsuccessfulButRolledBack(Set<Namespace> namespaces, UUID lockId) {
        log.error(
                "Failed to disable all namespaces. However, we successfully rolled back any partially disabled"
                        + " namespaces.",
                SafeArg.of("namespaces", namespaces),
                SafeArg.of("lockId", lockId));
        return DisableNamespacesResponse.unsuccessful(
                UnsuccessfulDisableNamespacesResponse.builder().build());
    }

    static DisableNamespacesResponse unsuccessfulAndRollBackFailed(Set<Namespace> namespaces, UUID lockId) {
        log.error(
                "Failed to disable all namespaces, and we may have failed to roll back some namespaces."
                        + " These may need to be force-re-enabled in order to return Timelock to a consistent state.",
                SafeArg.of("namespaces", namespaces),
                SafeArg.of("lockId", lockId));
        return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.builder()
                .partiallyDisabledNamespaces(namespaces)
                .build());
    }
}
