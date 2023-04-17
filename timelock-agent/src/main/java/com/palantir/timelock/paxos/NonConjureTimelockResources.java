/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.paxos;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.LockV1Resource;
import com.palantir.atlasdb.timelock.LockV1ResourceEndpoints;
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.TimestampManagementResource;
import com.palantir.atlasdb.timelock.TimestampManagementResourceEndpoints;
import com.palantir.atlasdb.timelock.TimestampResource;
import com.palantir.atlasdb.timelock.TimestampResourceEndpoints;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import java.util.Set;
import java.util.stream.Collectors;

public final class NonConjureTimelockResources {
    private NonConjureTimelockResources() {
        // utility
    }

    public static Set<UndertowService> createUndertowServices(
            TimelockNamespaces namespaces, RedirectRetryTargeter redirectRetryTargeter) {
        Set<UndertowService> rawServices = ImmutableSet.of(
                LockV1ResourceEndpoints.of(new LockV1Resource(namespaces)),
                TimestampResourceEndpoints.of(new TimestampResource(namespaces)),
                TimestampManagementResourceEndpoints.of(new TimestampManagementResource(namespaces)));

        return rawServices.stream()
                .map(service -> new TimelockUndertowExceptionWrapper(service, redirectRetryTargeter))
                .collect(Collectors.toSet());
    }

    public static Set<Object> createJerseyResources(TimelockNamespaces namespaces) {
        return ImmutableSet.of(
                new LockV1Resource(namespaces),
                new TimestampResource(namespaces),
                new TimestampManagementResource(namespaces));
    }
}
