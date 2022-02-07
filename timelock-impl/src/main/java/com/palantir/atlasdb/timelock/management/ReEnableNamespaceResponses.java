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

final class ReEnableNamespaceResponses {
    private static final SafeLogger log = SafeLoggerFactory.get(ReEnableNamespaceResponses.class);

    private ReEnableNamespaceResponses() {
        // utility class
    }

    static ReenableNamespacesResponse unsuccessfulDueToConsistentlyLockedNamespaces(
            Set<Namespace> consistentlyLockedNamespaces, Set<Namespace> partialFailures) {
        log.error(
                "Failed to re-enable all namespaces, because some namespace was consistently disabled "
                        + "with the wrong lock ID. This implies that this namespace being restored by another"
                        + " process. If that is the case, please either wait for that restore to complete, "
                        + " or kick off a restore without that namespace",
                SafeArg.of("consistentlyLockedNamespaces", consistentlyLockedNamespaces),
                SafeArg.of("partiallyLockedNamespaces", partialFailures));
        return ReenableNamespacesResponse.unsuccessful(UnsuccessfulReenableNamespacesResponse.builder()
                .consistentlyLockedNamespaces(consistentlyLockedNamespaces)
                .partiallyLockedNamespaces(partialFailures)
                .build());
    }

    static ReenableNamespacesResponse unsuccessfulWithPartiallyLockedNamespaces(
            Set<Namespace> partiallyLockedNamespaces) {
        log.error(
                "Failed to re-enable all namespaces, because some namespace was disabled "
                        + "with the wrong lock ID on some, but not all, nodes. "
                        + "Manual action may be required to unblock these namespaces.",
                SafeArg.of("lockedNamespaces", partiallyLockedNamespaces));
        return ReenableNamespacesResponse.unsuccessful(UnsuccessfulReenableNamespacesResponse.builder()
                .partiallyLockedNamespaces(partiallyLockedNamespaces)
                .build());
    }
}
