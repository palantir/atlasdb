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

import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulReenableNamespacesResponse;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Set;
import java.util.UUID;

final class ReEnableNamespaceResponses {
    private static final SafeLogger log = SafeLoggerFactory.get(ReEnableNamespaceResponses.class);

    private ReEnableNamespaceResponses() {
        // utility class
    }

    static ReenableNamespacesResponse unsuccessfulAndRollBackFailed(Set<Namespace> namespaces, UUID lockId) {
        log.error(
                "Failed to re-enable all namespaces, and we failed to roll back some partially re-enabled namespaces."
                        + " These will need to be force-re-enabled in order to return Timelock to a consistent state.",
                SafeArg.of("namespaces", namespaces),
                SafeArg.of("lockId", lockId));
        return ReenableNamespacesResponse.unsuccessful(UnsuccessfulReenableNamespacesResponse.builder()
                .partiallyLockedNamespaces(namespaces)
                .build());
    }

    static ReenableNamespacesResponse unsuccessfulButRolledBack(Set<Namespace> namespaces, UUID lockId) {
        log.error(
                "Failed to re-enable namespaces. However, we successfully rolled back any partially re-enabled"
                        + " namespaces",
                SafeArg.of("namespaces", namespaces),
                SafeArg.of("lockId", lockId));
        return ReenableNamespacesResponse.unsuccessful(
                UnsuccessfulReenableNamespacesResponse.builder().build());
    }

    static ReenableNamespacesResponse unsuccessfulDueToConsistentlyLockedNamespaces(
            Set<Namespace> consistentlyLockedNamespaces) {
        log.error(
                "Failed to re-enable all namespaces, because some namespace was consistently disabled "
                        + "with the wrong lock ID. This implies that this namespace being restored by another"
                        + " process. If that is the case, please either wait for that restore to complete, "
                        + " or kick off a restore without that namespace",
                SafeArg.of("lockedNamespaces", consistentlyLockedNamespaces));
        return ReenableNamespacesResponse.unsuccessful(UnsuccessfulReenableNamespacesResponse.builder()
                .consistentlyLockedNamespaces(consistentlyLockedNamespaces)
                .build());
    }

    static ReenableNamespacesResponse unsuccessfulDueToPingFailure(Set<Namespace> namespaces) {
        log.error(
                "Failed to reach all remote nodes. Not re-enabling any namespaces",
                SafeArg.of("namespaces", namespaces));
        return ReenableNamespacesResponse.unsuccessful(
                UnsuccessfulReenableNamespacesResponse.builder().build());
    }
}
